<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        Schema::create('annual_pass_batches', function (Blueprint $table) {
            $table->id();
            $table->string('batch_id')->unique();
            $table->string('status'); // processing, completed, failed
            $table->string('result_path')->nullable();
            $table->text('error_message')->nullable();
            $table->json('metrics')->nullable();
            $table->timestamps();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('annual_pass_batches');
    }
};
