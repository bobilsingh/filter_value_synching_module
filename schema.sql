-- ==========================================
-- Database Schema for Filter Synchronization
-- ==========================================

-- 1. Configuration Map Table
CREATE TABLE IF NOT EXISTS `dashboard_filter_value_sync_map` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `module_name` varchar(50) DEFAULT NULL,
  `dest_table_name` VARCHAR(255) NOT NULL,
  `dest_column_name` TEXT NOT NULL,
  `source_table_name` VARCHAR(255) NOT NULL,
  `source_column_name` TEXT NOT NULL,
  `date_column_name` VARCHAR(100) NOT NULL DEFAULT 'sourceTimeStamp',
  `group_by` VARCHAR(255) DEFAULT NULL,
  `frequency` VARCHAR(100) DEFAULT 'd-1',
  `sync_state` TEXT DEFAULT NULL,
  `is_active` TINYINT(1) NOT NULL DEFAULT 1,
  `last_sync_time` DATETIME DEFAULT NULL,
  `last_sync_status` ENUM('SUCCESS', 'FAILED', 'RUNNING') DEFAULT NULL,
  `rows_processed` INT NOT NULL DEFAULT 0,
  `rows_inserted` INT NOT NULL DEFAULT 0,
  `total_rows_processed` INT NOT NULL DEFAULT 0,
  `total_rows_inserted` INT NOT NULL DEFAULT 0,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 2. Execution History Log Table
CREATE TABLE IF NOT EXISTS `dashboard_filter_value_sync_log` (
  `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
  `map_id` INT NOT NULL,
  `run_time` DATETIME NOT NULL,
  `status` ENUM('SUCCESS', 'FAILED') NOT NULL,
  `rows_processed` INT DEFAULT 0,
  `rows_inserted` INT DEFAULT 0,
  `error_message` TEXT,
  INDEX (`map_id`),
  INDEX (`run_time`)
);