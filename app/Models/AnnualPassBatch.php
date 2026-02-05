<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasMany;

class AnnualPassBatch extends Model
{
    protected $fillable = [
        'batch_id',
        'status',
        'result_path',
        'error_message',
        'metrics',
    ];

    protected $casts = [
        'metrics' => 'array',
    ];

    public function files(): HasMany
    {
        return $this->hasMany(AnnualPassFile::class, 'batch_id', 'batch_id');
    }
}
