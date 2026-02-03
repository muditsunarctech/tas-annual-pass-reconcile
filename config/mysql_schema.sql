-- Annual Pass Reconciler - MySQL Database Schema
-- Purpose: Store reconciliation results for historical viewing and reporting

-- Create database (if needed)
CREATE DATABASE IF NOT EXISTS annual_pass_reconciler
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE annual_pass_reconciler;

-- Table 1: Reconciliation Runs
-- Stores metadata about each reconciliation execution
CREATE TABLE IF NOT EXISTS reconciliation_runs (
    run_id VARCHAR(50) PRIMARY KEY,
    run_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    data_source VARCHAR(20) NOT NULL COMMENT 'database or file_upload',
    bank VARCHAR(20) NOT NULL COMMENT 'IDFC or ICICI',
    project VARCHAR(50) NOT NULL,
    plaza_ids TEXT NOT NULL COMMENT 'Comma-separated plaza IDs',
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    total_transactions INT NOT NULL DEFAULT 0,
    total_nap INT NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'completed' COMMENT 'completed or failed',
    created_by VARCHAR(100) DEFAULT NULL,
    INDEX idx_run_date (run_date),
    INDEX idx_bank (bank),
    INDEX idx_project (project),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Table 2: Reconciliation Transactions
-- Stores individual transactions with TripCount
CREATE TABLE IF NOT EXISTS reconciliation_transactions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(50) NOT NULL,
    plaza_id VARCHAR(10) NOT NULL,
    plaza_name VARCHAR(100) NOT NULL,
    vehicle_reg_no VARCHAR(50) NOT NULL,
    tag_id VARCHAR(100) NOT NULL,
    transaction_time TIMESTAMP NOT NULL,
    trip_count INT NOT NULL,
    report_date DATE NOT NULL,
    is_qualified_nap TINYINT(1) NOT NULL DEFAULT 0 COMMENT '0=ATP, 1=NAP',
    trip_type VARCHAR(50) DEFAULT NULL,
    FOREIGN KEY (run_id) REFERENCES reconciliation_runs(run_id) ON DELETE CASCADE,
    INDEX idx_run_id (run_id),
    INDEX idx_plaza_id (plaza_id),
    INDEX idx_vehicle_reg_no (vehicle_reg_no),
    INDEX idx_report_date (report_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Table 3: Reconciliation Daily Summary
-- Stores daily ATP/NAP summaries by plaza
CREATE TABLE IF NOT EXISTS reconciliation_daily_summary (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(50) NOT NULL,
    project_name VARCHAR(50) NOT NULL,
    plaza_id VARCHAR(10) NOT NULL,
    plaza_name VARCHAR(100) NOT NULL,
    report_date DATE NOT NULL,
    atp INT NOT NULL DEFAULT 0 COMMENT 'Annual Pass count',
    nap INT NOT NULL DEFAULT 0 COMMENT 'NAP count',
    FOREIGN KEY (run_id) REFERENCES reconciliation_runs(run_id) ON DELETE CASCADE,
    INDEX idx_run_id (run_id),
    INDEX idx_project_name (project_name),
    INDEX idx_plaza_id (plaza_id),
    INDEX idx_report_date (report_date),
    UNIQUE KEY unique_run_plaza_date (run_id, plaza_id, report_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Grant permissions (adjust as needed)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON annual_pass_reconciler.* TO 'app_user'@'%';
