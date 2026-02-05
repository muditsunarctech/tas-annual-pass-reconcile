<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

class AnnualPassFile extends Model
{
    protected $fillable = [
        'batch_id',
        'file_path',
        'original_name',
        'size',
    ];

    public function batch(): BelongsTo
    {
        return $this->belongsTo(AnnualPassBatch::class, 'batch_id', 'batch_id');
    }
}
