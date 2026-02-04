<?php

namespace App\Services;

use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Storage;
use Illuminate\Support\Facades\Cache;
use OpenSpout\Reader\XLSX\Reader as XLSXReader;
use OpenSpout\Reader\CSV\Reader as CSVReader;
use OpenSpout\Writer\XLSX\Writer as XLSXWriter;
use OpenSpout\Common\Entity\Row;
use PhpOffice\PhpSpreadsheet\Shared\Date;

class ReconciliationService
{
    protected $batchId;
    protected $config;

    public function __construct()
    {
        $this->config = config('reconciliation');
    }

    /**
     * Main Entry Point
     */
    public function reconcile($batchId, $filePaths, $outputDir)
    {
        $this->batchId = $batchId;
        $this->log("Starting reconciliation service for batch: $batchId");

        // 1. Consolidate and Group (Project -> Plaza)
        $groupedTransactions = $this->consolidateFiles($filePaths);

        $this->metrics = ['total_atp' => 0, 'total_nap' => 0];

        $finalSummary = [];
        $finalDetails = [];

        // 2. Process Groups
        foreach ($groupedTransactions as $project => $plazas) {
            foreach ($plazas as $plazaId => $txns) {
                // Sort by VRN then Time
                usort($txns, function ($a, $b) {
                    $c = strcmp($a['vrn'], $b['vrn']);
                    if ($c !== 0) return $c;
                    return $a['ts'] <=> $b['ts'];
                });

                // Apply Logic
                $processed = $this->applySlidingWindow($txns);

                // Aggregate Financials (Metrics updated inside)
                $summaryFunc = $this->aggregateResults($project, $plazaId, $processed);

                // Merge for Output
                foreach ($summaryFunc as $sRow) {
                    $finalSummary[] = $sRow;
                }
                foreach ($processed as $pRow) {
                    $finalDetails[] = $pRow['row_data'];
                }
            }
        }

        // 3. Write Excel
        $outputFile = $this->writeExcel($finalSummary, $finalDetails, $outputDir);

        return [
            'file' => $outputFile,
            'metrics' => $this->metrics
        ];
    }

    protected $metrics = [];

    /**
     * Reads all files and groups them by Project -> Plaza
     * Uses OpenSpout for low memory usage.
     */
    protected function consolidateFiles($filePaths)
    {
        $grouped = [];

        foreach ($filePaths as $path) {
            try {
                $absPath = Storage::disk('local')->path($path);
                $ext = strtolower(pathinfo($absPath, PATHINFO_EXTENSION));

                if ($ext === 'csv') {
                    $reader = new CSVReader();
                } else {
                    $reader = new XLSXReader();
                }
                $reader->open($absPath);

                $headerMap = [];
                $plazaColIdx = -1;
                $configHeaders = $this->config['plaza_id_headers'];

                foreach ($reader->getSheetIterator() as $sheet) {
                    foreach ($sheet->getRowIterator() as $row) {
                        $cells = $row->getCells();
                        $data = [];
                        foreach ($cells as $cell) {
                            $val = $cell->getValue();
                            // Handle DateTime objects from Spout
                            if ($val instanceof \DateTimeInterface) {
                                $val = $val->format('Y-m-d H:i:s');
                            }
                            $data[] = $val;
                        }

                        // Header Detection
                        if ($plazaColIdx === -1) {
                            foreach ($configHeaders as $ph) {
                                $key = array_search($ph, $data);
                                if ($key !== false) {
                                    $headerMap = array_flip($data);
                                    $plazaColIdx = $key;
                                    break;
                                }
                            }
                            continue;
                        }

                        // Process Row
                        $plazaId = $data[$plazaColIdx] ?? null;
                        if (!$plazaId) continue;

                        list($bank, $plazaName, $project) = $this->resolvePlaza($plazaId);
                        if (!$bank) continue;

                        $parsed = $this->parseRow($data, $headerMap, $bank, $plazaId, $project);
                        if ($parsed) {
                            $grouped[$project][$plazaId][] = $parsed;
                        }
                    }
                }
                $reader->close();
            } catch (\Exception $e) {
                $this->log("Error reading file $path: " . $e->getMessage());
            }
        }
        return $grouped;
    }

    protected function parseRow($data, $headerMap, $bank, $plazaId, $project)
    {
        $outMap = $this->config['output_columns'][$bank] ?? [];
        $res = [];

        // Extract Standard Columns
        foreach ($outMap as $stdKey => $candidates) {
            $val = '';
            foreach ((array)$candidates as $cand) {
                if (isset($headerMap[$cand])) {
                    $val = $data[$headerMap[$cand]] ?? '';
                    break;
                }
            }
            $res[$stdKey] = trim($val);
        }

        // Must have Time & VRN
        if (empty($res['TransactionDateTime']) || empty($res['VRN'])) return null;

        // Parse Date
        $ts = $this->parseDate($res['TransactionDateTime']);
        if (!$ts) return null;

        $res['TransactionDateTime'] = date('Y-m-d H:i:s', $ts);
        $res['PlazaID'] = $plazaId;

        return [
            'vrn' => $res['VRN'],
            'ts' => $ts,
            'row_data' => $res, // Standardized Row
            'project' => $project,
            'bank' => $bank
        ];
    }

    protected function applySlidingWindow($txns)
    {
        $n = count($txns);
        $i = 0;

        while ($i < $n) {
            $j = $i;
            $currVRN = $txns[$i]['vrn'];

            // Find group for this VRN
            while ($j < $n && $txns[$j]['vrn'] === $currVRN) {
                $j++;
            }

            // Process VRN Group
            $windowStart = null;
            $windowEnd = null;
            $tripCount = 0;

            for ($k = $i; $k < $j; $k++) {
                $ts = $txns[$k]['ts'];

                if ($windowStart === null || $ts > $windowEnd) {
                    $windowStart = $ts;
                    $windowEnd = $ts + (24 * 3600);
                    $tripCount = 1;
                } else {
                    $tripCount++;
                }

                // Add Calculated Fields
                $txns[$k]['row_data']['TripCount'] = $tripCount;
                $txns[$k]['row_data']['IsQualifiedNAP'] = ($tripCount <= 2) ? 1 : 0;

                // Report Date (8AM Logic)
                $hour = (int)date('H', $ts);
                $d = date('Y-m-d', $ts);
                if ($hour < 8) {
                    $d = date('Y-m-d', $ts - 86400);
                }
                $txns[$k]['row_data']['ReportDate'] = $d;
            }

            $i = $j;
        }
        return $txns;
    }

    protected function aggregateResults($project, $plazaId, $processed)
    {
        $daily = []; // [Date => [ATP, NAP]]
        $plazaName = $this->resolvePlaza($plazaId)[1];

        foreach ($processed as $p) {
            $date = $p['row_data']['ReportDate'];
            if (!isset($daily[$date])) $daily[$date] = ['atp' => 0, 'nap' => 0];

            $daily[$date]['atp']++;
            if ($p['row_data']['IsQualifiedNAP'] == 1) {
                $daily[$date]['nap']++;
            }
        }

        $summary = [];
        $constant = $this->config['plaza_constant'];
        $fare = $this->config['fares'][$plazaId] ?? 0;

        foreach ($daily as $date => $counts) {
            $this->metrics['total_atp'] += $counts['atp'];
            $this->metrics['total_nap'] += $counts['nap'];

            $comp = $counts['nap'] * $constant * $fare;
            $summary[] = [
                'Project' => $project,
                'Plaza ID' => $plazaId,
                'Plaza Name' => $plazaName,
                'Report Date' => $date,
                'Total ATP' => $counts['atp'],
                'Total NAP' => $counts['nap'],
                'Plaza Constant' => $constant,
                'Single Side Fare' => $fare,
                'Compensation Amount' => number_format($comp, 2, '.', '')
            ];
        }
        return $summary;
    }

    protected function writeExcel($summaryRows, $detailRows, $outputDir)
    {
        $fileName = "reconciliation_results_" . $this->batchId . ".xlsx";
        $filePath = $outputDir . "/" . $fileName;
        $absPath = Storage::disk('local')->path($filePath);

        // Ensure dir exists
        $dir = dirname($absPath);
        if (!is_dir($dir)) mkdir($dir, 0777, true);

        $writer = new XLSXWriter();
        $writer->openToFile($absPath);

        // --- SHEET 1: SUMMARY ---
        $sheet1 = $writer->getCurrentSheet();
        $sheet1->setName('Summary');

        if (!empty($summaryRows)) {
            $headers = array_keys($summaryRows[0]);
            $writer->addRow(Row::fromValues($headers));
            foreach ($summaryRows as $row) {
                $writer->addRow(Row::fromValues(array_values($row)));
            }
        }

        // --- SHEET 2: DETAILS ---
        $writer->addNewSheetAndMakeItCurrent();
        $sheet2 = $writer->getCurrentSheet();
        $sheet2->setName('Details');

        if (!empty($detailRows)) {
            $headers = array_keys($detailRows[0]);
            $writer->addRow(Row::fromValues($headers));
            foreach ($detailRows as $row) {
                $writer->addRow(Row::fromValues(array_values($row)));
            }
        }

        $writer->close();
        return $filePath;
    }

    // Helpers...
    protected function resolvePlaza($id)
    {
        $id = strval(intval(floatval(trim($id, " '\""))));
        $id = str_pad($id, 6, "0", STR_PAD_LEFT);

        foreach ($this->config['bank_plaza_map'] as $bank => $plazas) {
            if (isset($plazas[$id])) {
                return [$bank, $plazas[$id][0], $plazas[$id][1]];
            }
        }
        return [null, null, null];
    }

    protected function parseDate($dateStr)
    {
        if (is_numeric($dateStr)) {
            return Date::excelToTimestamp($dateStr);
        }
        $ts = strtotime($dateStr);
        if (!$ts) {
            $formats = ['d-m-Y H:i:s', 'd/m/Y H:i:s', 'Y-m-d H:i:s', 'Y-m-d'];
            foreach ($formats as $fmt) {
                $d = \DateTime::createFromFormat($fmt, $dateStr);
                if ($d) return $d->getTimestamp();
            }
        }
        return $ts;
    }

    protected function log($msg)
    {
        Log::info($msg);
    }
}
