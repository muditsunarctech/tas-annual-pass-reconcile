<?php

namespace App\Jobs;

use App\Services\ReconciliationService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Storage;
use ZipArchive;
use App\Models\AnnualPassBatch;

class ProcessAnnualPassReconciliation implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    protected $filePaths;

    protected $batchId;

    protected $outputDir;

    protected $tempDir;

    /**
     * Create a new job instance.
     */
    public function __construct(array $filePaths, string $batchId)
    {
        $this->filePaths = $filePaths;
        $this->batchId = $batchId;
    }

    protected function addLog($message)
    {
        $key = 'annual_pass_logs_'.$this->batchId;
        $logs = Cache::get($key, []);
        $logs[] = '['.date('H:i:s').'] '.$message;
        Cache::put($key, $logs, 3600);
        Log::info("[$this->batchId] $message");
    }

    /**
     * Execute the job.
     */
    public function handle(): void
    {
        try {
            $this->addLog('Starting reconciliation pipeline (Optimized)...');

            // Directories
            $batchBase = 'temp/annual-pass/'.$this->batchId;
            $this->outputDir = $batchBase.'/OUTPUT';
            $this->tempDir = $batchBase;

            Storage::disk('local')->makeDirectory($this->outputDir);

            // Delegate to Service
            $service = new ReconciliationService;

            // Reconcile
            $this->addLog('Processing files...');
            $result = $service->reconcile($this->batchId, $this->filePaths, $this->outputDir);

            $xlsxRelativePath = $result['file'];
            $metrics = $result['metrics'];

            // Create Zip (Controller expects Zip)
            $this->addLog('Creating ZIP archive...');
            $zipRelativePath = $this->createZip($xlsxRelativePath);

            // Update Status in cache (legacy) and DB
            Cache::put('annual_pass_status_'.$this->batchId, 'completed', 3600);
            Cache::put('annual_pass_result_'.$this->batchId, 'public/'.$zipRelativePath, 3600);
            Cache::put('annual_pass_metrics_'.$this->batchId, $metrics, 3600);

            // Persist to DB
            $batch = AnnualPassBatch::where('batch_id', $this->batchId)->first();
            if ($batch) {
                $batch->status = 'completed';
                $batch->result_path = $zipRelativePath; // path relative to public disk
                $batch->metrics = $metrics;
                $batch->save();
            }

            $this->addLog('Processing complete.');
        } catch (\Exception $e) {
            $this->addLog('Error: '.$e->getMessage());
            Log::error('Reconciliation failed: '.$e->getMessage());
            Log::error($e->getTraceAsString());
            Cache::put('annual_pass_status_'.$this->batchId, 'failed', 3600);
            Cache::put('annual_pass_error_'.$this->batchId, $e->getMessage(), 3600);

            $batch = AnnualPassBatch::where('batch_id', $this->batchId)->first();
            if ($batch) {
                $batch->status = 'failed';
                $batch->error_message = $e->getMessage();
                $batch->save();
            }
        } finally {
            // Cleanup if needed
            // Storage::disk('local')->deleteDirectory($this->tempDir);
        }
    }

    protected function createZip($xlsxRelativePath)
    {
        $zipName = 'reconciliation_results_'.$this->batchId.'.zip';
        $zipPath = storage_path('app/public/'.$zipName);

        // Ensure directory exists
        if (! is_dir(dirname($zipPath))) {
            mkdir(dirname($zipPath), 0777, true);
        }


        $zip = new ZipArchive;
        if ($zip->open($zipPath, ZipArchive::CREATE | ZipArchive::OVERWRITE) === true) {
            // Add any CSVs from the OUTPUT directory
            $outputLocalDir = Storage::disk('local')->path($this->outputDir);
            if (is_dir($outputLocalDir)) {
                $it = new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator($outputLocalDir, \FilesystemIterator::SKIP_DOTS));
                foreach ($it as $file) {
                    if (! $file->isFile()) continue;
                    $ext = strtolower(pathinfo($file->getFilename(), PATHINFO_EXTENSION));
                    if ($ext !== 'csv') continue;
                    $abs = $file->getRealPath();
                    $innerRel = ltrim(str_replace($outputLocalDir . DIRECTORY_SEPARATOR, '', $abs), DIRECTORY_SEPARATOR);
                    $zip->addFile($abs, $innerRel);
                }
            }

            $zip->close();
        }

        // Return filename relative to public disk root
        return $zipName;
    }
}
