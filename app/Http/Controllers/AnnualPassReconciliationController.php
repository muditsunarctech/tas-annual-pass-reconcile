<?php

namespace App\Http\Controllers;

use App\Jobs\ProcessAnnualPassReconciliation;
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
        $statusKey = $this->statusKey($batchId);
        $status = Cache::get($statusKey);

        if (! $status) {
            return redirect()
                ->route('annual-pass.index')
                ->with('error', 'Batch expired or invalid.');
        }

        $viewData = [
            'status'  => $status,
            'batchId' => $batchId,
            'logs'    => Cache::get('annual_pass_logs_' . $batchId, [])
        ];

        if ($status === 'completed') {
            $resultPath = Cache::get($this->resultKey($batchId));

            if (! $resultPath || ! Storage::exists($resultPath)) {
                $viewData['status'] = 'error';
                $viewData['message'] = 'Result file missing.';
            } else {
                $viewData['downloadUrl'] = route('annual-pass.download', $batchId);
                $viewData['metrics'] = Cache::get('annual_pass_metrics_' . $batchId);
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
        $resultPath = Cache::get($this->resultKey($batchId));

        abort_unless(
            $resultPath && Storage::exists($resultPath),
            404,
            'Result file not found.'
        );

        return Storage::download(
            $resultPath,
            "reconciliation_results_{$batchId}.xlsx"
        );
    }

    /*
    |--------------------------------------------------------------------------
    | Cache Key Helpers
    |--------------------------------------------------------------------------
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
