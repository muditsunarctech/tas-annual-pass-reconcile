<?php

use Illuminate\Support\Facades\Route;


use App\Http\Controllers\AnnualPassReconciliationController;

Route::get('/', function () {
    return view('annual-pass.index');
});

Route::prefix('annual-pass')->name('annual-pass.')->group(function () {
    Route::get('/', [AnnualPassReconciliationController::class, 'index'])->name('index');
    Route::post('/process', [AnnualPassReconciliationController::class, 'process'])->name('process');
    Route::get('/status/{batchId}', [AnnualPassReconciliationController::class, 'status'])->name('status');
    Route::get('/download/{batchId}', [AnnualPassReconciliationController::class, 'download'])->name('download');
});
