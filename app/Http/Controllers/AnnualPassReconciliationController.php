<?php

namespace App\Http\Controllers;

use App\Jobs\ProcessAnnualPassReconciliation;
use App\Models\AnnualPassBatch;
use App\Models\AnnualPassFile;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Storage;
use Illuminate\Support\Str;

class AnnualPassReconciliationController extends Controller
{
    private const STATUS_TTL = 3600; // 1 hour
    private const CACHE_PREFIX = 'annual_pass_';
    private const TEMP_DIR = 'temp/annual-pass';

    /**
     * Show upload form.
     */
    public function index()
    {
        return view('annual-pass.index');
    }

    /**
     * Handle file upload and dispatch job.
     */
    public function process(Request $request)
    {
        $validated = $request->validate([
            'files'   => ['required', 'array', 'min:1'],
            'files.*' => ['file', 'max:102400'], // 100MB per file
        ]);

        $batchId = (string) Str::uuid();
        $storedFiles = [];

        // Cleanup previous batches before starting new one
        $this->cleanupPreviousBatches();

        // Persist batch record
        AnnualPassBatch::create([
            'batch_id' => $batchId,
            'status' => 'processing',
        ]);

        foreach ($validated['files'] as $file) {
            if (! $file->isValid()) {
                Log::warning('Invalid file skipped', [
                    'batch_id' => $batchId,
                    'name'     => $file->getClientOriginalName(),
                    'error'    => $file->getErrorMessage(),
                ]);
                continue;
            }

            // Store using the 'local' disk explicitly to ensure consistency
            $path = $file->store(self::TEMP_DIR . '/' . $batchId, 'local');

            if (! Storage::disk('local')->exists($path)) {
                Log::error('File storage failed', [
                    'batch_id' => $batchId,
                    'path'     => $path,
                ]);
                continue;
            }

            Log::info('File uploaded', [
                'batch_id' => $batchId,
                'name'     => $file->getClientOriginalName(),
                'size'     => $file->getSize(),
                'path'     => $path,
            ]);

            $storedFiles[] = $path;

            // Persist file record
            AnnualPassFile::create([
                'batch_id' => $batchId,
                'file_path' => $path,
                'original_name' => $file->getClientOriginalName(),
                'size' => $file->getSize(),
            ]);
        }

        if (empty($storedFiles)) {
            return back()->withErrors([
                'files' => 'No valid files were uploaded.',
            ]);
        }

        Cache::put($this->statusKey($batchId), 'processing', self::STATUS_TTL);

        // Change to default queue to avoid worker confusion
        ProcessAnnualPassReconciliation::dispatch($storedFiles, $batchId);

        return redirect()->route('annual-pass.status', $batchId);
    }

    /**
     * Poll batch status.
     */
    public function status(string $batchId)
    {
        // Prefer DB for authoritative batch status
        $batch = AnnualPassBatch::where('batch_id', $batchId)->first();
        if (! $batch) {
            return redirect()
                ->route('annual-pass.index')
                ->with('error', 'Batch expired or invalid.');
        }

        $status = $batch->status;

        $viewData = [
            'status'  => $status,
            'batchId' => $batchId,
            'logs'    => Cache::get('annual_pass_logs_' . $batchId, []),
        ];

        if ($status === 'completed') {
            // Prefer DB-stored path
            $batch = \App\Models\AnnualPassBatch::where('batch_id', $batchId)->first();
            $disk = Storage::disk('public');
            $ok = false;
            if ($batch && $batch->result_path && $disk->exists($batch->result_path)) {
                $ok = true;
            } else {
                $cached = Cache::get($this->resultKey($batchId));
                if ($cached && strpos($cached, 'public/') === 0) {
                    $publicPath = substr($cached, 7);
                    if ($disk->exists($publicPath)) {
                        $ok = true;
                    }
                }
            }

            if (! $ok) {
                $viewData['status'] = 'error';
                $viewData['message'] = 'Result file missing.';
            } else {
                $viewData['downloadUrl'] = route('annual-pass.download', $batchId);
                $viewData['metrics'] = Cache::get('annual_pass_metrics_' . $batchId) ?? ($batch->metrics ?? null);
            }
        }

        if ($status === 'failed') {
            $viewData['message'] = Cache::get($this->errorKey($batchId), 'Processing failed unexpectedly.');
        }

        return view('annual-pass.status', $viewData);
    }

    /**
     * Download reconciliation result.
     */
    public function download(string $batchId)
    {
        // Prefer DB for authoritative result path
        $batch = \App\Models\AnnualPassBatch::where('batch_id', $batchId)->first();

        $resultPath = null;
        if ($batch && $batch->result_path) {
            $resultPath = $batch->result_path; // relative path on public disk
            $disk = Storage::disk('public');
            abort_unless(
                $resultPath && $disk->exists($resultPath),
                404,
                'Result file not found.'
            );

            return $disk->download($resultPath, "reconciliation_results_{$batchId}.zip");
        }

        // Fallback to cache (legacy)
        $cached = Cache::get($this->resultKey($batchId));
        if ($cached && strpos($cached, 'public/') === 0) {
            $publicPath = substr($cached, 7);
            $disk = Storage::disk('public');
            abort_unless($disk->exists($publicPath), 404, 'Result file not found.');

            return $disk->download($publicPath, "reconciliation_results_{$batchId}.zip");
        }

        abort(404, 'Result file not found.');
    }

    private function cleanupPreviousBatches(): void
    {
        try {
            Log::info('Cleaning up previous batches as requested.');

            $batches = AnnualPassBatch::all();

            foreach ($batches as $batch) {
                // 1. Storage: Cleanup local temp files
                $localPath = self::TEMP_DIR . '/' . $batch->batch_id;
                if (Storage::disk('local')->exists($localPath)) {
                    Storage::disk('local')->deleteDirectory($localPath);
                }

                // 2. Storage: Cleanup public result files
                if ($batch->result_path && Storage::disk('public')->exists($batch->result_path)) {
                    Storage::disk('public')->delete($batch->result_path);
                }

                // 3. Cache: Clear all associated keys
                Cache::forget($this->statusKey($batch->batch_id));
                Cache::forget($this->resultKey($batch->batch_id));
                Cache::forget($this->errorKey($batch->batch_id));
                Cache::forget(self::CACHE_PREFIX . "metrics_{$batch->batch_id}");
                Cache::forget(self::CACHE_PREFIX . "logs_{$batch->batch_id}");

                // 4. DB: Delete the batch (cascade will delete files)
                $batch->delete();
            }

            // Ensure the base directory is clean of any orphaned files
            if (Storage::disk('local')->exists(self::TEMP_DIR)) {
                $subDirs = Storage::disk('local')->directories(self::TEMP_DIR);
                foreach ($subDirs as $dir) {
                    Storage::disk('local')->deleteDirectory($dir);
                }
            }

            Log::info('Cleanup complete.');
        } catch (\Exception $e) {
            Log::error('Batch cleanup failed: ' . $e->getMessage());
        }
    }

    /*
    |--------------------------------------------------------------------------
    | Cache Key Helpers
    |--------------------------------------------------------------------------
    |
    */

    private function statusKey(string $batchId): string
    {
        return self::CACHE_PREFIX . "status_{$batchId}";
    }

    private function resultKey(string $batchId): string
    {
        return self::CACHE_PREFIX . "result_{$batchId}";
    }

    private function errorKey(string $batchId): string
    {
        return self::CACHE_PREFIX . "error_{$batchId}";
    }
}
