# Database Filter Value Synchronization ETL Tool - User Guide

This tool is a high-performance, dynamic, and multi-threaded ETL (Extract, Transform, Load) module designed to synchronize filter values from one or more source tables into a single destination table inside a MySQL database.

---

## 🌟 Key Features

1. **Smart Auto-Scheduling**: Run a single cron job every minute. The script checks database timestamps (`sync_state` column) and automatically decides which mappings actually need to run based on their configured frequencies.
2. **Dynamic Frequency Lookbacks**: Supports any custom frequency string:
   - **Minute-based**: `1 minute` (looks back 2 mins to prevent overlap), `5 minute`, `15 minute`, `120 minute`, etc.
   - **Hour-based**: `hourly` (looks back 1 hour), `Xh` (looks back X hours).
   - **Day-based**: `daily` / `yesterday` (looks back 1 day), `d-1`, `d-2`, `d-15` (looks back arbitrary X days).
   - **Full History**: `all` / `full` / `history` (truncates destination table once first, and pulls all unique records from source without date filtering).
   - Case-insensitive support (e.g., `5 MINUTE`, `Daily`, `d-1`, `All` are all matched).
3. **Multi-Source Tables Sync**: Supply a comma-separated list of source tables (e.g., `orders_retail, orders_wholesale`) to sync data from all of them into a single destination table.
4. **Prior Schema Verification**: Before running a sync query, the script queries `INFORMATION_SCHEMA` to verify that all source and destination tables and columns exist.
5. **Connection Pooling**: Utilizes thread-safe connection pooling to handle multiple sync mappings simultaneously in parallel threads.
6. **Dry-Run Mode**: Inspect generated SQL queries and count matching records without changing anything in your database.
7. **Execution History Logging**: Logs detailed execution stats (rows processed, rows inserted, status, errors) for each source table and frequency in a log history table.
8. **Daily Auto-Cleanup**: Automatically deletes file-system log backups older than 7 days, and database execution log rows older than 7 days.

---

## 📁 Project File Structure

```text
filter_sync_module/
├── config.py             # Loads settings from .env / config.json
├── database.py           # Handles thread-safe MySQL connection pooling
├── logger.py             # Configures TimedRotating log files
├── sync_filters.py       # Core ETL sync logic, parsing, and runner
├── schema.sql            # Database schema for config & log tables
├── run.sh                # Executable shell runner script
└── .env                  # Configuration file (created from .env.example)
```

---

## ⚙️ Setup and Installation

### Step 1: Install Python Dependencies
The project requires Python 3 and the `mysql-connector-python` library. 

* **If you have root/administrator privileges:**
  ```bash
  sudo dnf install python3-pip   # On Rocky/RedHat/CentOS Linux
  # OR
  sudo apt install python3-pip   # On Ubuntu/Debian Linux
  
  pip3 install -r requirements.txt
  ```

* **If you DO NOT have root/administrator privileges (Non-root user installation):**
  If pip is missing, download and install it locally in your home directory:
  ```bash
  curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
  python3 get-pip.py --user
  ```
  Then, install the MySQL connector locally to your user account:
  ```bash
  python3 -m pip install --user -r requirements.txt
  ```

### Step 2: Initialize Database Tables
Execute the following SQL commands in your MySQL query editor to create the configuration mapping and execution log tables:

```sql
-- 1. Configuration Map Table
CREATE TABLE IF NOT EXISTS `dashboard_filter_value_sync_map` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `module_name` VARCHAR(50) DEFAULT NULL,
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
```

### Step 3: Configure Environment Variables
Copy the template `.env` file and configure your credentials:
```bash
cp .env.example .env
```
Open `.env` in a text editor and fill in your connection details:
```env
DB_HOST=localhost
DB_PORT=3306
DB_USER=your_db_username
DB_PASSWORD=your_db_password
DB_NAME=your_config_database
DB_DATA_NAME=your_data_database
MAX_WORKERS=4
```

### Step 4: Grant Shell Permissions
Make the runner script executable:
```bash
chmod +x run.sh
```

---

## 🚀 CLI Commands & Options

| Command | Description |
| :--- | :--- |
| `./run.sh` | **Smart Auto-Schedule Mode**. Checks all active configurations and runs only those whose frequencies are due. |
| `./run.sh --dry-run` | **Safe Preview Mode**. Generates and prints the SQL queries to logs/terminal without editing database. |
| `./run.sh --frequency "5 minute"` | **Override Frequency**. Executes only configurations matching `5 minute` frequency instantly. |
| `./run.sh --config-id 3` | **Override Config SNO**. Executes configuration ID `3` immediately. |
| `./run.sh --date 2026-07-15` | **Override Date**. Force-runs daily sync mappings for a specific date. |

---

## 📝 Complete Step-by-Step Example

Let's assume we want to sync unique categories from two sales tables (`sales_offline` and `sales_online`) into a single dashboard filter table (`dashboard_categories`).

### 1. Configure the Mapping Row in Database
Insert a configuration row into `dashboard_filter_value_sync_map`:
* **`module_name`**: `"Category Filter"`
* **`source_table_name`**: `"sales_offline, sales_online"` *(Multi-source)*
* **`source_column_name`**: `"category_id, category_name"`
* **`dest_table_name`**: `"dashboard_categories"`
* **`dest_column_name`**: `"id, name"`
* **`date_column_name`**: `"created_at"` *(This is used to check frequency range)*
* **`frequency`**: `"5 minute, daily"` *(Runs every 5 minutes and also daily)*
* **`is_active`**: `1`

### 2. Perform a Dry Run
Before running it live, execute a dry run to inspect the SQL:
```bash
./run.sh --dry-run
```

**Terminal Output Logs will show:**
```text
2026-07-16 18:00:00 [INFO] [MainThread] - Starting database filter synchronization job...
2026-07-16 18:00:00 [INFO] [MainThread] - !!! DRY-RUN MODE ACTIVE: No database changes will be committed !!!
2026-07-16 18:00:01 [INFO] [ThreadPoolExecutor-0_0] - Config #1 (Category Filter): Processing 2 source table(s) on due frequencies: ['5 minute', 'daily']
2026-07-16 18:00:01 [INFO] [ThreadPoolExecutor-0_0] - Config #1: Checking sales_offline -> dashboard_categories | Frequency: 5 minute (Minute lookback: last 5 minute(s))
2026-07-16 18:00:01 [INFO] [ThreadPoolExecutor-0_0] - [DRY-RUN] Config #1 Query:
    INSERT IGNORE INTO `pview`.`dashboard_categories` (`id`, `name`)
    SELECT s.`category_id` AS `id`, s.`category_name` AS `name`
    FROM `pview`.`sales_offline` s
    WHERE s.`created_at` >= DATE_SUB(NOW(), INTERVAL 5 MINUTE)
      AND NOT EXISTS (
          SELECT 1 
          FROM `pview`.`dashboard_categories` d 
          WHERE d.`id` <=> s.`category_id` AND d.`name` <=> s.`category_name`
      )
2026-07-16 18:00:01 [INFO] [ThreadPoolExecutor-0_0] - [DRY-RUN] Config #1: Estimated source rows: 12
```

### 3. Run Live Execution
Run without arguments to start the real synchronization:
```bash
./run.sh
```
* On the first run, both `5 minute` and `daily` frequencies are due, so both run and update `sync_state` in the database to today's date/time.
* If you run it again 1 minute later, the logs will show:
  ```text
  Config #1 | Freq: 5 minute: Skipped (elapsed 60s < 300s).
  Config #1 | Freq: daily: Skipped (already run today).
  Config #1: No frequencies are due at this time. Skipping configuration.
  ```

---

## ⏰ Cron Job Setup (Production Scheduling)
To deploy this tool in production, configure a single cron job to execute the script **every minute**. 

1. Open your crontab editor:
   ```bash
   crontab -e
   ```
2. Add the following line at the end:
   ```cron
   * * * * * /path/to/filter_sync_module/run.sh >> /path/to/filter_sync_module/logs/cron_run.log 2>&1
   ```
The Python script will execute every minute, checking `sync_state` column to run `5 minute` rows every 5 minutes, `hourly` rows once per hour, and `daily`/`d-X` rows once per calendar day.
>>>>>>> c5a7937 (Initial commit - Advanced ETL Filter Value Sync Engine)
