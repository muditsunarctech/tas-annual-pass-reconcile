<?php

namespace App\Services;

use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Storage;
use OpenSpout\Common\Entity\Row;
use OpenSpout\Reader\CSV\Reader as CSVReader;
use OpenSpout\Reader\XLSX\Reader as XLSXReader;
use OpenSpout\Writer\XLSX\Writer as XLSXWriter;
use PhpOffice\PhpSpreadsheet\Shared\Date;

class ReconciliationService
{
    protected string $batchId;

    protected array $config;

    protected array $metrics = [
        'total_atp' => 0,
        'total_nap' => 0,
    ];

    public function __construct()
    {
        $this->config = config('reconciliation');
    }

    /**
     * Orchestrates the full reconciliation flow.
     */
    public function reconcile(string $batchId, array $filePaths, string $outputDir): array
    {
        $this->batchId = $batchId;
        $this->metrics = ['total_atp' => 0, 'total_nap' => 0];

        $this->log("Reconciliation started for batch {$batchId}");

        $grouped = $this->consolidateFiles($filePaths);

        $finalSummary = [];
        $finalDetails = [];

        foreach ($grouped as $project => $plazas) {
            foreach ($plazas as $plazaId => $txns) {

                usort($txns, fn ($a, $b) => [$a['vrn'], $a['ts']] <=> [$b['vrn'], $b['ts']]
                );

                $processed = $this->applySlidingWindow($txns);

                $finalSummary = array_merge(
                    $finalSummary,
                    $this->aggregateResults($project, $plazaId, $processed)
                );

                foreach ($processed as $row) {
                    // Enrich row_data with Bank / PlazaName / ProjectName for export
                    $detail = $row['row_data'];
                    [$bank, $plazaName, $projectName] = $this->resolvePlaza($detail['PlazaID'] ?? $plazaId);
                    $detail['Bank'] = $bank ?? $row['bank'] ?? null;
                    $detail['PlazaName'] = $plazaName;
                    $detail['ProjectName'] = $projectName ?? $project;

                    $finalDetails[] = $detail;
                }
            }
        }

        $file = $this->writeExcel($finalSummary, $finalDetails, $outputDir);

        return [
            'file' => $file,
            'metrics' => $this->metrics,
        ];
    }

    /**
     * Reads all input files and groups data by Project → Plaza.
     */
    protected function consolidateFiles(array $filePaths): array
    {
        $grouped = [];

        foreach ($filePaths as $path) {
            try {
                $absPath = Storage::disk('local')->path($path);
                $ext = strtolower(pathinfo($absPath, PATHINFO_EXTENSION));

                $reader = $ext === 'csv' ? new CSVReader : new XLSXReader;
                $reader->open($absPath);

                foreach ($reader->getSheetIterator() as $sheet) {
                    $headerMap = null;
                    $plazaCol = null;
                    $skippedByFilter = 0;
                    $totalRows = 0;

                    foreach ($sheet->getRowIterator() as $row) {
                        $values = array_map(
                            function ($val) {
                                if ($val instanceof \DateTimeInterface) {
                                    return $val->format('Y-m-d H:i:s');
                                }

                                return trim((string) $val);
                            },
                            $row->toArray()
                        );

                        if ($headerMap === null) {
                            $upperHeaders = array_map('strtoupper', $values);
                            $headerMap = array_flip($upperHeaders);
                            $plazaCol = $this->findPlazaColumn($headerMap);
                            // If we couldn't find a plaza column, this row is likely metadata/junk.
                            if ($plazaCol === null) {
                                $headerMap = null;
                            } else {
                                $colName = array_search($plazaCol, $headerMap);
                                $this->log("Header detected in {$path}. Plaza column name: '{$colName}', index: {$plazaCol}. Headers found: ".json_encode(array_keys($headerMap)));
                            }

                            continue;
                        }

                        if ($plazaCol === null) {
                            continue;
                        }

                        $plazaId = $values[$plazaCol] ?? null;
                        if (! $plazaId) {
                            continue;
                        }

                        [$bank, $plazaName, $project] = $this->resolvePlaza($plazaId);
                        if (! $bank) {
                            if ($totalRows < 5) {
                                $this->log("Row skipped: resolvePlaza failed for ID '{$plazaId}'. Found values: ".json_encode($values));
                            }

                            continue;
                        }

                        // Filter to ANNUAL PASS transactions only, mirroring the original slicer logic
                        $reasonCol = $this->findReasonColumn($headerMap, $bank);
                        if ($reasonCol !== null) {
                            $reasonValue = strtoupper(trim((string) ($values[$reasonCol] ?? '')));
                            $validValues = array_map('strtoupper', $this->config['annual_pass_values'] ?? []);
                            if (! in_array($reasonValue, $validValues, true)) {
                                $skippedByFilter++;

                                continue;
                            }
                        } else {
                            // If reason column is not found for a bank that normally has it, we might want to log it
                            // but for now we follow the logic: no reason column -> no filter applied.
                        }

                        $parsed = $this->parseRow($values, $headerMap, $bank, $plazaId, $project);
                        if ($parsed) {
                            $grouped[$project][$plazaId][] = $parsed;
                            $totalRows++;
                        } else {
                            if ($totalRows < 5) {
                                $this->log("Row skipped: parseRow failed for ID '{$plazaId}' (Bank: {$bank})");
                            }
                        }
                    }
                }

                $projStats = [];
                foreach ($grouped as $proj => $plazas) {
                    $projStats[$proj] = 0;
                    foreach ($plazas as $pid => $txns) {
                        $projStats[$proj] += count($txns);
                    }
                }
                $this->log("Completed reading {$path}. Current stats: ".json_encode($projStats)." (Skipped by filter: {$skippedByFilter})");

                $reader->close();
            } catch (\Throwable $e) {
                $this->log("File read failed ({$path}): {$e->getMessage()}");
            }
        }

        return $grouped;
    }

    protected function findPlazaColumn(array $headerMap): ?int
    {
        foreach ($this->config['plaza_id_headers'] as $header) {
            $upperHeader = strtoupper(trim($header));
            if (isset($headerMap[$upperHeader])) {
                return $headerMap[$upperHeader];
            }
        }

        return null;
    }

    /**
     * Locate the "reason code" column for the given bank so we can
     * filter only ANNUAL PASS transactions.
     */
    protected function findReasonColumn(array $headerMap, string $bank): ?int
    {
        $bankMap = $this->config['bank_column_map'][$bank]['FastagReasonCode'] ?? null;
        if ($bankMap === null) {
            return null;
        }

        foreach ((array) $bankMap as $candidate) {
            $upperCandidate = strtoupper(trim($candidate));
            if (isset($headerMap[$upperCandidate])) {
                return $headerMap[$upperCandidate];
            }
        }

        return null;
    }

    protected function parseRow(array $data, array $headerMap, string $bank, string $plazaId, string $project): ?array
    {
        $mapping = $this->config['output_columns'][$bank] ?? [];
        $row = [];

        foreach ($mapping as $key => $candidates) {
            foreach ((array) $candidates as $candidate) {
                $upperCandidate = strtoupper(trim($candidate));
                if (isset($headerMap[$upperCandidate])) {
                    $row[$key] = $data[$headerMap[$upperCandidate]] ?? '';
                    break;
                }
            }
            $row[$key] = trim($row[$key] ?? '');
        }

        if (empty($row['TransactionDateTime']) || empty($row['VRN'])) {
            return null;
        }

        $ts = $this->parseDate($row['TransactionDateTime']);
        if (! $ts) {
            return null;
        }

        $row['TransactionDateTime'] = date('Y-m-d H:i:s', $ts);
        $row['PlazaID'] = $plazaId;

        return [
            'vrn' => $row['VRN'],
            'ts' => $ts,
            'row_data' => $row,
            'project' => $project,
            'bank' => $bank,
        ];
    }

    protected function applySlidingWindow(array $txns): array
    {
        $count = count($txns);
        $i = 0;

        while ($i < $count) {
            $j = $i;

            while ($j < $count && $txns[$j]['vrn'] === $txns[$i]['vrn']) {
                $j++;
            }

            $windowEnd = 0;
            $tripCount = 0;

            for ($k = $i; $k < $j; $k++) {
                $ts = $txns[$k]['ts'];

                if ($ts > $windowEnd) {
                    $windowEnd = $ts + 86400;
                    $tripCount = 1;
                } else {
                    $tripCount++;
                }

                $reportDate = date('H', $ts) < 8
                    ? date('Y-m-d', $ts - 86400)
                    : date('Y-m-d', $ts);

                $txns[$k]['row_data'] += [
                    'TripCount' => $tripCount,
                    'IsQualifiedNAP' => $tripCount <= 2 ? 1 : 0,
                    'ReportDate' => $reportDate,
                ];
            }

            $i = $j;
        }

        return $txns;
    }

    protected function aggregateResults(string $project, string $plazaId, array $processed): array
    {
        $daily = [];
        [, $plazaName] = $this->resolvePlaza($plazaId);

        foreach ($processed as $row) {
            $date = $row['row_data']['ReportDate'];
            $daily[$date]['atp'] = ($daily[$date]['atp'] ?? 0) + 1;

            if ($row['row_data']['IsQualifiedNAP']) {
                $daily[$date]['nap'] = ($daily[$date]['nap'] ?? 0) + 1;
            }
        }

        $constant = $this->config['plaza_constant'];
        $fare = $this->config['fares'][$plazaId] ?? 0;

        $summary = [];

        foreach ($daily as $date => $c) {
            $this->metrics['total_atp'] += $c['atp'];
            $this->metrics['total_nap'] += $c['nap'] ?? 0;

            $summary[] = [
                // Match Streamlit CSV: ProjectName,PlazaID,PlazaName,ReportDate,ATP,NAP
                'ProjectName' => $project,
                'PlazaID' => $plazaId,
                'PlazaName' => $plazaName,
                'ReportDate' => $date,
                'ATP' => $c['atp'],
                'NAP' => $c['nap'] ?? 0,
            ];
        }

        return $summary;
    }

    protected function writeExcel(array $summary, array $details, string $outputDir): string
    {
        $file = "{$outputDir}/reconciliation_results_{$this->batchId}.xlsx";
        $abs = Storage::disk('local')->path($file);

        if (! is_dir(dirname($abs))) {
            mkdir(dirname($abs), 0777, true);
        }

        $writer = new XLSXWriter;
        $writer->openToFile($abs);

        if ($summary) {
            $writer->getCurrentSheet()->setName('Summary');
            $writer->addRow(Row::fromValues(array_keys($summary[0])));
            foreach ($summary as $row) {
                $writer->addRow(Row::fromValues(array_values($row)));
            }
        }

        if ($details) {
            $writer->addNewSheetAndMakeItCurrent();
            $writer->getCurrentSheet()->setName('Details');
            $writer->addRow(Row::fromValues(array_keys($details[0])));
            foreach ($details as $row) {
                $writer->addRow(Row::fromValues(array_values($row)));
            }
        }

        $writer->close();

        // Also emit CSV outputs alongside the XLSX for downstream consumption.
        try {
            // Summary CSV
            if (! empty($summary)) {
                $csvName1 = 'IHPL_daily_ATP_NAP_plaza.csv';
                $csvPath1 = $outputDir.'/'.$csvName1;
                $absCsv1 = Storage::disk('local')->path($csvPath1);
                $dir1 = dirname($absCsv1);
                if (! is_dir($dir1)) {
                    mkdir($dir1, 0777, true);
                }
                $h1 = fopen($absCsv1, 'w');
                // Fixed header order to match reference CSV
                $headers = ['ProjectName', 'PlazaID', 'PlazaName', 'ReportDate', 'ATP', 'NAP'];
                fputcsv($h1, $headers);
                foreach ($summary as $row) {
                    $line = [
                        $row['ProjectName'] ?? '',
                        $row['PlazaID'] ?? '',
                        $row['PlazaName'] ?? '',
                        $row['ReportDate'] ?? '',
                        $row['ATP'] ?? 0,
                        $row['NAP'] ?? 0,
                    ];
                    fputcsv($h1, $line);
                }
                fclose($h1);
            }

            // Details CSV (transactions with TripCount)
            if (! empty($details)) {
                $csvName2 = 'IHPL_transactions_with_tripcount.csv';
                $csvPath2 = $outputDir.'/'.$csvName2;
                $absCsv2 = Storage::disk('local')->path($csvPath2);
                $dir2 = dirname($absCsv2);
                if (! is_dir($dir2)) {
                    mkdir($dir2, 0777, true);
                }
                $h2 = fopen($absCsv2, 'w');
                // Match reference detail CSV:
                // Reader Read Time,Vehicle Reg. No.,Tag ID,PlazaID,TripType,Bank,PlazaName,ProjectName,TripCount,ReportDate,IsQualifiedNAP
                $headers2 = [
                    'Reader Read Time',
                    'Vehicle Reg. No.',
                    'Tag ID',
                    'PlazaID',
                    'TripType',
                    'Bank',
                    'PlazaName',
                    'ProjectName',
                    'TripCount',
                    'ReportDate',
                    'IsQualifiedNAP',
                ];
                fputcsv($h2, $headers2);
                foreach ($details as $row) {
                    $line = [
                        $row['TransactionDateTime'] ?? '',
                        $row['VRN'] ?? '',
                        $row['TagID'] ?? '',
                        $row['PlazaID'] ?? '',
                        $row['TripType'] ?? '',
                        $row['Bank'] ?? '',
                        $row['PlazaName'] ?? '',
                        $row['ProjectName'] ?? '',
                        $row['TripCount'] ?? 0,
                        $row['ReportDate'] ?? '',
                        // Convert 1/0 (or truthy) to 'True' / 'False' like Streamlit output
                        ! empty($row['IsQualifiedNAP']) ? 'True' : 'False',
                    ];
                    fputcsv($h2, $line);
                }
                fclose($h2);
            }
        } catch (\Exception $e) {
            // Log but don't fail the entire operation
            Log::warning('Failed to write CSV outputs: '.$e->getMessage());
        }

        return $file;
    }

    protected function resolvePlaza($id): array
    {
        $raw = trim((string) $id);
        $raw = trim($raw, "'\""); // strip quotes or apostrophes used for Excel formatting

        if (ctype_digit($raw)) {
            // Case 1: Pure digits
            // If it's longer than 6 digits (e.g. a transaction ID), take the first 6
            if (strlen($raw) > 6) {
                $raw = substr($raw, 0, 6);
            }
        } else {
            // Case 2: Mixed or float
            if (str_contains($raw, '.')) {
                $floatVal = floatval($raw);
                if ($floatVal > 0) {
                    $raw = (string) intval($floatVal);
                }
            }

            if (! ctype_digit($raw)) {
                $intVal = intval($raw);
                if ($intVal <= 0) {
                    return [null, null, null];
                }
                $raw = (string) $intVal;
            }
        }

        $id = str_pad($raw, 6, '0', STR_PAD_LEFT);

        foreach ($this->config['bank_plaza_map'] as $bank => $plazas) {
            // Use integer comparison if numeric string to be safe with PHP array keys
            if (isset($plazas[$id])) {
                return [$bank, $plazas[$id][0], $plazas[$id][1]];
            }
        }

        return [null, null, null];
    }

    protected function parseDate($value): ?int
    {
        if (is_numeric($value)) {
            return Date::excelToTimestamp($value);
        }

        return strtotime($value) ?: null;
    }

    protected function log(string $msg): void
    {
        Log::info("[Reconciliation] {$msg}");
    }
}
