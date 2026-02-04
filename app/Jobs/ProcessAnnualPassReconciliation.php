<?php

namespace App\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Storage;
use PhpOffice\PhpSpreadsheet\IOFactory;
use ZipArchive;
use Carbon\Carbon;

class ProcessAnnualPassReconciliation implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    protected $filePaths;
    protected $batchId;
    protected $tempDir;
    protected $slicedDir;
    protected $mergedDir;
    protected $outputDir;
    protected $fileHandles = []; 

    // Constants (Ported from implementation plan)
    const PLAZA_ID_HEADERS = ["PLAZA_ID", " Plaza ID", "Entry Plaza Code", "Entry Plaza Id", " Plaza Code", " Entry Plaza Code"];
    const ANNUAL_PASS_VALUES = ["ANNUALPASS", "ANNUAL PASS"];

    const BANK_PLAZA_MAP = [
        "IDFC" => [
            "142001" => ["Ghoti", "IHPL"], "142002" => ["Arjunali", "IHPL"],
            "220001" => ["Raipur", "BPPTPL"], "220002" => ["Indranagar", "BPPTPL"],
            "220003" => ["Birami", "BPPTPL"], "220004" => ["Uthman", "BPPTPL"],
            "235001" => ["Mandawada", "SUTPL"], "235002" => ["Negadiya", "SUTPL"],
            "243000" => ["Rupakheda", "BRTPL"], "243001" => ["Mujras", "BRTPL"],
            "073001" => ["Bollapalli", "SEL"], "073002" => ["Tangutur", "SEL"],
            "073003" => ["Musunur", "SEL"]
        ],
        "ICICI" => [
            "540030" => ["Ladgaon", "CSJTPL"], "540032" => ["Nagewadi", "CSJTPL"],
            "120001" => ["Shanthigrama", "DHTPL"], "120002" => ["Kadabahalli", "DHTPL"],
            "139001" => ["Shirpur", "DPTL"], "139002" => ["Songir", "DPTL"],
            "167001" => ["Vaniyambadi", "KWTPL"], "167002" => ["Pallikonda", "KWTPL"],
            "169001" => ["Palayam", "KTTRL"], "234002" => ["Chagalamarri", "REPL"],
            "352001" => ["Nannur", "REPL"], "352013" => ["Chapirevula", "REPL"],
            "352065" => ["Patimeedapalli", "REPL"], "045001" => ["Gudur", "HYTPL"],
            "046001" => ["Kasaba", "BHTPL"], "046002" => ["Nagarhalla", "BHTPL"],
            "079001" => ["Shakapur", "WATL"]
        ]
    ];

    const BANK_COLUMN_MAP = [
        "ICICI" => ["FastagReasonCode" => ["Reason", "Reason Code"]],
        "IDFC" => ["FastagReasonCode" => " Trc Vrc Reason Code"]
    ];

    const OUTPUT_COLUMNS = [
        "ICICI" => [
            "TransactionDateTime" => ["Transaction Date", "Entry Txn Date"],
            "VRN" => ["Licence Plate No.", "License Plate No."],
            "TagID" => ["Tag Id", "Hex Tag No"],
            "TripType" => ["Trip Type", "TRIPTYPEDISCRIPTION"]
        ],
        "IDFC" => [
            "TransactionDateTime" => ["READER_READ_TIME", " Reader Read Time"],
            "VRN" => ["VEHICLE_REG_NO", " Vehicle Reg. No."],
            "TagID" => ["TAG_ID", " Tag ID"],
            "TripType" => ["JOURNEY_TYPE", " Journey Type"]
        ]
    ];

    protected $totalAtp = 0;
    protected $totalNap = 0;

    /**
     * Create a new job instance.
     */
    public function __construct(array $filePaths, string $batchId)
    {
        $this->filePaths = $filePaths;
        $this->batchId = $batchId;
    }

    protected function addLog($message) {
        $key = 'annual_pass_logs_' . $this->batchId;
        $logs = Cache::get($key, []);
        $logs[] = "[" . date('H:i:s') . "] " . $message;
        Cache::put($key, $logs, 3600);
        Log::info("[$this->batchId] $message");
    }

    /**
     * Execute the job.
     */
    public function handle(): void
    {
        try {
            $this->addLog('Starting reconciliation pipeline...');
            
            // Paths relative to the 'local' disk root
            $batchBase = 'temp/annual-pass/' . $this->batchId;
            $this->slicedDir = $batchBase . '/SLICED';
            $this->mergedDir = $batchBase . '/MERGED';
            $this->outputDir = $batchBase . '/RECONCILIATION_OUTPUT';

            // Create directories using the Storage facade
            Storage::disk('local')->makeDirectory($this->slicedDir);
            Storage::disk('local')->makeDirectory($this->mergedDir);
            Storage::disk('local')->makeDirectory($this->outputDir);

            $this->addLog('Slicing files...');
            $this->runSlicer();
            
            $this->addLog('Merging monthly data...');
            $this->runMerger();
            
            $this->addLog('Reconciling transactions...');
            $this->runReconciler();
            
            $this->addLog('Creating ZIP archive...');
            $zipRelativePath = $this->createZip();
            
            Cache::put('annual_pass_status_' . $this->batchId, 'completed', 3600);
            Cache::put('annual_pass_result_' . $this->batchId, $zipRelativePath, 3600);
            Cache::put('annual_pass_metrics_' . $this->batchId, [
                'total_atp' => $this->totalAtp,
                'total_nap' => $this->totalNap
            ], 3600);

            $this->addLog('Processing complete.');

        } catch (\Exception $e) {
            $this->addLog('Error: ' . $e->getMessage());
            Log::error("Reconciliation failed: " . $e->getMessage());
            Log::error($e->getTraceAsString());
            Cache::put('annual_pass_status_' . $this->batchId, 'failed', 3600);
            Cache::put('annual_pass_error_' . $this->batchId, $e->getMessage(), 3600);
        } finally {
             foreach ($this->fileHandles as $fh) {
                 if (is_resource($fh)) fclose($fh);
             }
             $this->fileHandles = [];
             // File::deleteDirectory($this->tempDir); // Uncomment in prod
        }
    }

    protected function writeRow($path, $row, $headers) {
        if (!isset($this->fileHandles[$path])) {
            $absPath = Storage::disk('local')->path($path);
            $exists = file_exists($absPath);
            $this->fileHandles[$path] = fopen($absPath, 'a');
            if (!$exists) fputcsv($this->fileHandles[$path], $headers);
        }
        fputcsv($this->fileHandles[$path], $row);
    }

    protected function resolvePlaza($plazaId) {
        $plazaId = strval(intval(floatval(trim($plazaId, " '\"")))); // Simple cleanup
        $plazaId = str_pad($plazaId, 6, "0", STR_PAD_LEFT);
        
        foreach (self::BANK_PLAZA_MAP as $bank => $plazas) {
            if (isset($plazas[$plazaId])) {
                return [$bank, $plazas[$plazaId][0], $plazas[$plazaId][1]];
            }
        }
        return [null, null, null];
    }
    
    protected function normalizeColumnName($col) {
        return trim(preg_replace('/\s+/', ' ', str_replace(["\n", "\t"], ' ', $col)));
    }

    protected function runSlicer() {
        foreach ($this->filePaths as $filePath) {
            // Get absolute path from Storage facade
            $absPath = Storage::disk('local')->path($filePath);
            $fileName = basename($filePath);
            
            if (!Storage::disk('local')->exists($filePath)) {
                $this->addLog("Warning: File not found - $fileName");
                continue;
            }

            $this->addLog("Processing $fileName...");
            $ext = strtolower(pathinfo($absPath, PATHINFO_EXTENSION));

            if ($ext === 'csv') {
                $this->processCsv($absPath);
            } else {
                try {
                    $this->processExcel($absPath);
                } catch (\Exception $e) {
                    $this->addLog("Error in $fileName: " . $e->getMessage());
                    throw $e;
                }
            }
        }
    }

    protected function processCsv($filePath) {
        if (($handle = fopen($filePath, "r")) !== FALSE) {
            $headers = fgetcsv($handle, 0, ",");
            if (!$headers) { fclose($handle); return; }
            
            // Normalize headers map
            $headerMap = [];
            foreach ($headers as $idx => $h) {
                $headerMap[$this->normalizeColumnName($h)] = $idx;
            }

            // Find Plaza ID Column (normalize config keys too)
            $plazaColIdx = -1;
            foreach (self::PLAZA_ID_HEADERS as $pHeader) {
                $norm = $this->normalizeColumnName($pHeader);
                if (isset($headerMap[$norm])) {
                    $plazaColIdx = $headerMap[$norm];
                    break;
                }
            }

            if ($plazaColIdx === -1) { fclose($handle); return; }

            // We need to read the first row to determine Bank/Plaza
            $firstRow = fgetcsv($handle, 0, ",");
            if (!$firstRow) { fclose($handle); return; }

            $plazaId = $firstRow[$plazaColIdx];
            list($bank, $plazaName, $projectName) = $this->resolvePlaza($plazaId);

            if (!$bank) { fclose($handle); return; }

            // Find Reason Column
            $reasonColIdx = -1;
            $reasonConfig = self::BANK_COLUMN_MAP[$bank]['FastagReasonCode'] ?? null;
            if ($reasonConfig) {
                 if (is_array($reasonConfig)) {
                     foreach($reasonConfig as $rc) {
                         $rcn = $this->normalizeColumnName($rc);
                         if (isset($headerMap[$rcn])) { $reasonColIdx = $headerMap[$rcn]; break; }
                     }
                 } else {
                     $rcn = $this->normalizeColumnName($reasonConfig);
                     if (isset($headerMap[$rcn])) $reasonColIdx = $headerMap[$rcn];
                 }
            }

            // Output Columns Mapping
            $outputMap = []; // 'StdName' => Index
            foreach (self::OUTPUT_COLUMNS[$bank] as $stdName => $colConfig) {
                 if (is_array($colConfig)) {
                     foreach($colConfig as $cc) {
                         $ccn = $this->normalizeColumnName($cc);
                         if (isset($headerMap[$ccn])) { $outputMap[$stdName] = $headerMap[$ccn]; break; }
                     }
                 } else {
                     $ccn = $this->normalizeColumnName($colConfig);
                     if (isset($headerMap[$ccn])) $outputMap[$stdName] = $headerMap[$ccn];
                 }
            }
            $outputMap['PlazaID'] = $plazaColIdx;

            // Rewind and process
            rewind($handle);
            fgetcsv($handle); // Skip header

            while (($row = fgetcsv($handle, 0, ",")) !== FALSE) {
                 // Filter by Annual Pass
                 if ($reasonColIdx !== -1) {
                     $val = strtoupper(trim($row[$reasonColIdx] ?? ''));
                     if (!in_array($val, self::ANNUAL_PASS_VALUES)) continue;
                 }
                 
                 // Extract Data
                 $outRow = [];
                 foreach ($outputMap as $key => $idx) {
                     $outRow[$key] = trim($row[$idx] ?? '');
                 }

                 // Determine Month-Year
                 $dateStr = $outRow['TransactionDateTime'] ?? '';
                 $monthYear = $this->extractMonthYear($dateStr);
                 
                 if ($monthYear && $projectName) {
                    $dir = $this->slicedDir . "/$monthYear/$projectName";
                    Storage::disk('local')->makeDirectory($dir);
                    $outfile = "$dir/{$plazaName}_ANNUALPASS.csv";
                    
                    $standardized = $this->standardizeRow($outRow);
                    $this->writeRow($outfile, $standardized, array_keys($standardized));
                 }
            }
            fclose($handle);
        }
    }

    protected function processExcel($filePath) {
        ini_set('memory_limit', '2048M');
        
        $reader = IOFactory::createReaderForFile($filePath);
        $reader->setReadDataOnly(true);
        $spreadsheet = $reader->load($filePath);
        
        foreach ($spreadsheet->getSheetNames() as $sheetName) {
            $sheet = $spreadsheet->getSheetByName($sheetName);
            
             // Naive header detection (try first 3 rows)
            $headerRowIdx = -1;
            $headerMap = [];
            
            $rowCount = 0;
            foreach ($sheet->getRowIterator() as $row) {
                $rowCount++;
                if ($rowCount > 3 && $headerRowIdx === -1) break;
                
                $cells = [];
                $cellIterator = $row->getCellIterator();
                $cellIterator->setIterateOnlyExistingCells(false);
                foreach ($cellIterator as $cell) {
                    $cells[] = $cell->getValue();
                }
                
                $tempMap = [];
                foreach ($cells as $idx => $h) {
                    $tempMap[$this->normalizeColumnName($h)] = $idx;
                }
                
                foreach (self::PLAZA_ID_HEADERS as $pHeader) {
                    $phn = $this->normalizeColumnName($pHeader);
                    if (isset($tempMap[$phn])) {
                        $headerRowIdx = $rowCount;
                        $headerMap = $tempMap;
                        break 2;
                    }
                }
            }
            
            if ($headerRowIdx === -1) continue;

            $plazaColIdx = -1;
             foreach (self::PLAZA_ID_HEADERS as $pHeader) {
                $phn = $this->normalizeColumnName($pHeader);
                if (isset($headerMap[$phn])) {
                    $plazaColIdx = $headerMap[$phn];
                    break;
                }
            }
            
             // Process Rows
            $rowCount = 0;
            foreach ($sheet->getRowIterator() as $row) {
                $rowCount++;
                if ($rowCount <= $headerRowIdx) continue;
                
                $cells = [];
                $cellIterator = $row->getCellIterator();
                $cellIterator->setIterateOnlyExistingCells(false);
                foreach ($cellIterator as $cell) {
                    $cells[] = $cell->getValue();
                }
                
                $pid = $cells[$plazaColIdx] ?? null;
                if (!$pid) continue;

                list($bank, $plazaName, $projectName) = $this->resolvePlaza($pid);
                if (!$bank) continue;

                // Column mapping for this bank
                $reasonColIdx = -1;
                $reasonConfig = self::BANK_COLUMN_MAP[$bank]['FastagReasonCode'] ?? null;
                if ($reasonConfig) {
                    if (is_array($reasonConfig)) {
                         foreach($reasonConfig as $rc) {
                             $rcn = $this->normalizeColumnName($rc);
                             if (isset($headerMap[$rcn])) { $reasonColIdx = $headerMap[$rcn]; break; }
                         }
                    } else {
                         $rcn = $this->normalizeColumnName($reasonConfig);
                         if (isset($headerMap[$rcn])) $reasonColIdx = $headerMap[$rcn];
                    }
                }

                if ($reasonColIdx !== -1) {
                    $val = strtoupper(trim($cells[$reasonColIdx] ?? ''));
                    if (!in_array($val, self::ANNUAL_PASS_VALUES)) continue;
                }

                $outputMap = [];
                foreach (self::OUTPUT_COLUMNS[$bank] as $stdName => $colConfig) {
                    if (is_array($colConfig)) {
                         foreach($colConfig as $cc) {
                             $ccn = $this->normalizeColumnName($cc);
                             if (isset($headerMap[$ccn])) { $outputMap[$stdName] = $headerMap[$ccn]; break; }
                         }
                    } else {
                         $ccn = $this->normalizeColumnName($colConfig);
                         if (isset($headerMap[$ccn])) $outputMap[$stdName] = $headerMap[$ccn];
                    }
                }
                $outputMap['PlazaID'] = $plazaColIdx;

                $outRow = [];
                foreach ($outputMap as $key => $idx) {
                    $outRow[$key] = trim($cells[$idx] ?? '');
                }
                
                $dateStr = $outRow['TransactionDateTime'] ?? '';
                $monthYear = $this->extractMonthYear($dateStr);
                $outRow['TransactionDateTime'] = $this->normalizeDate($dateStr);
                
                if ($monthYear && $projectName) {
                    $dir = $this->slicedDir . "/$monthYear/$projectName";
                    Storage::disk('local')->makeDirectory($dir);
                    $outfile = "$dir/{$plazaName}_ANNUALPASS.csv";
                    
                    $standardized = $this->standardizeRow($outRow);
                    $this->writeRow($outfile, $standardized, array_keys($standardized));
                }
            }
        }
    }

    protected function extractMonthYear($dateStr) {
        $ts = $this->parseDate($dateStr);
        return $ts ? date('M-y', $ts) : null;
    }
    
    protected function normalizeDate($dateStr) {
        $ts = $this->parseDate($dateStr);
        return $ts ? date('Y-m-d H:i:s', $ts) : $dateStr;
    }

    protected function parseDate($dateStr) {
        if (is_numeric($dateStr)) {
            // Excel Serial Date
             return \PhpOffice\PhpSpreadsheet\Shared\Date::excelToTimestamp($dateStr);
        }
        $ts = strtotime($dateStr);
        if (!$ts) {
             // Try some formats
             $formats = ['d-m-Y H:i:s', 'd/m/Y H:i:s', 'Y-m-d H:i:s'];
             foreach ($formats as $fmt) {
                 $d = \DateTime::createFromFormat($fmt, $dateStr);
                 if ($d) return $d->getTimestamp();
             }
        }
        return $ts;
    }

    protected function standardizeRow($row) {
        // Simple cleanup
        foreach ($row as &$val) {
            if (is_string($val)) {
                $val = preg_replace('/\s+/', ' ', $val);
            }
        }
        return $row;
    }

    protected function runMerger() {
        // Traverse sliced dir: Month -> Project -> Files
        $months = Storage::disk('local')->directories($this->slicedDir);
        foreach ($months as $monthDir) {
             $month = basename($monthDir);
             $projects = Storage::disk('local')->directories($monthDir);
             foreach ($projects as $projectPath) {
                 $project = basename($projectPath);
                 $outProjectDir = $this->mergedDir . '/' . $project;
                 Storage::disk('local')->makeDirectory($outProjectDir);
                 
                 $files = Storage::disk('local')->files($projectPath);
                 foreach ($files as $filePath) {
                     if (strpos($filePath, '.csv') === false) continue;
                     
                     $fileName = basename($filePath);
                     $destPath = $outProjectDir . '/' . $fileName; 
                     
                     $absSrcPath = Storage::disk('local')->path($filePath);
                     $fp = fopen($absSrcPath, 'r');
                     $header = fgetcsv($fp);
                     if ($header) {
                        while($row = fgetcsv($fp)) {
                            if (count($header) === count($row)) {
                                $rowAssoc = array_combine($header, $row);
                                $rowAssoc['SourceMonth'] = $month;
                                // Append to dest
                                $this->writeRow($destPath, $rowAssoc, array_merge($header, ['SourceMonth']));
                            }
                        }
                     }
                     fclose($fp);
                 }
             }
        }
    }

    protected function runReconciler() {
         $projects = Storage::disk('local')->directories($this->mergedDir);
         foreach ($projects as $projectPath) {
             $project = basename($projectPath);
             $files = Storage::disk('local')->files($projectPath);
             
             $projectTxns = [];
             
             foreach ($files as $filePath) {
                 if (strpos($filePath, '.csv') === false) continue;
                 $absFilePath = Storage::disk('local')->path($filePath);
                 $fp = fopen($absFilePath, 'r');
                 $header = fgetcsv($fp);
                 while($row = fgetcsv($fp)) {
                     if (count($header) === count($row)) {
                        $projectTxns[] = array_combine($header, $row);
                     }
                 }
                 fclose($fp);
             }
             
             if (empty($projectTxns)) continue;
             
             // Process Project Transactions
             // 1. Sort by PlazaID, VRN, Time
             usort($projectTxns, function($a, $b) {
                 $c = strcmp($a['PlazaID'], $b['PlazaID']);
                 if ($c !== 0) return $c;
                 $c = strcmp($a['VRN'], $b['VRN']);
                 if ($c !== 0) return $c;
                 return strcmp($a['TransactionDateTime'], $b['TransactionDateTime']);
             });
             
             // 2. Trip Count Logic
             // We process consecutively for each (Plaza, VRN) group
             $n = count($projectTxns);
             $i = 0;
             while ($i < $n) {
                 $j = $i;
                 $currPlaza = $projectTxns[$i]['PlazaID'];
                 $currVRN = $projectTxns[$i]['VRN'];
                 
                 // Identify group
                 while ($j < $n && $projectTxns[$j]['PlazaID'] === $currPlaza && $projectTxns[$j]['VRN'] === $currVRN) {
                     $j++;
                 }
                 
                 // Process group [i, j-1]
                 $windowStart = null;
                 $windowEnd = null;
                 $tripCount = 0;
                 
                 for ($k = $i; $k < $j; $k++) {
                     $ts = strtotime($projectTxns[$k]['TransactionDateTime']);
                     
                     if ($windowStart === null || $ts > $windowEnd) {
                         $windowStart = $ts;
                         $windowEnd = $ts + (24 * 3600);
                         $tripCount = 1;
                     } else {
                         $tripCount++;
                     }
                     $projectTxns[$k]['TripCount'] = $tripCount;
                     $projectTxns[$k]['IsQualifiedNAP'] = ($tripCount <= 2) ? 1 : 0;
                     
                     // Report Date
                     $hour = (int)date('H', $ts);
                     $rptDate = date('Y-m-d', $ts);
                     if ($hour < 8) {
                         $rptDate = date('Y-m-d', $ts - 86400);
                     }
                     $projectTxns[$k]['ReportDate'] = $rptDate;
                 }
                 
                 $i = $j;
             }
             
             // 3. Generate Summary
             $summary = [];
             foreach ($projectTxns as $txn) {
                 list($bank, $pName, $projName) = $this->resolvePlaza($txn['PlazaID']);
                 
                 $rptDate = $txn['ReportDate'] ?? 'Unknown';
                 $k = "$projName|{$txn['PlazaID']}|$pName|$rptDate";
                 if (!isset($summary[$k])) {
                     $summary[$k] = ['ATP' => 0, 'NAP' => 0];
                 }
                 $summary[$k]['ATP']++;
                 $this->totalAtp++;
                 if ($txn['IsQualifiedNAP']) {
                     $summary[$k]['NAP']++;
                     $this->totalNap++;
                 }
             }
             
             // SAVE
             $projOutDir = $this->outputDir . '/' . $project;
             Storage::disk('local')->makeDirectory($projOutDir);
             
             $txnFile = $projOutDir . "/{$project}_transactions_with_tripcount.csv";
             $sumFile = $projOutDir . "/{$project}_daily_ATP_NAP_plaza.csv";
             
             foreach ($projectTxns as $txn) {
                 $this->writeRow($txnFile, $txn, array_keys($txn));
             }
             
             $sumRows = [];
             foreach ($summary as $key => $counts) {
                 list($prj, $pid, $pname, $date) = explode('|', $key);
                 $sumRows[] = [
                     'ProjectName' => $prj,
                     'PlazaID' => $pid,
                     'PlazaName' => $pname,
                     'ReportDate' => $date,
                     'ATP' => $counts['ATP'],
                     'NAP' => $counts['NAP']
                 ];
             }
             
             foreach ($sumRows as $row) {
                 $this->writeRow($sumFile, $row, array_keys($row));
             }
             
             // Explicitly clear memory
             unset($projectTxns);
             unset($summary);
             unset($sumRows);
             gc_collect_cycles();
         }
    }

    protected function createZip() {
        $zipFile = storage_path('app/public/reconciliation_results_' . $this->batchId . '.zip');
        $zip = new ZipArchive();
        $zip->open($zipFile, ZipArchive::CREATE | ZipArchive::OVERWRITE);
        
        $absOutputDir = Storage::disk('local')->path($this->outputDir);
        $files = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($absOutputDir),
            \RecursiveIteratorIterator::LEAVES_ONLY
        );

        foreach ($files as $name => $file) {
            if (!$file->isDir()) {
                $filePath = $file->getRealPath();
                $relativePath = substr($filePath, strlen($absOutputDir) + 1);
                $zip->addFile($filePath, $relativePath);
            }
        }
        $zip->close();
        return 'public/reconciliation_results_' . $this->batchId . '.zip';
    }
}
