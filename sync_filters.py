import sys
import re
import argparse
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from mysql.connector import Error
from database import DatabaseConnection
from logger import logger
from config import DB_DATA_NAME, MAX_WORKERS, DEFAULT_FREQUENCY

def get_table_with_db(table_name, default_db):
    """Helper to wrap table with database name if not already prefixed."""
    if "." in table_name:
        return table_name
    return f"`{default_db}`.`{table_name}`"

def resolve_frequency_condition(frequency_str, date_col):
    """
    Parses a frequency string and returns:
      1. SQL WHERE condition clause
      2. Boolean indicating if it is day-based
      3. Description string
      4. Target range string
    """
    freq = frequency_str.strip().lower()
    
    # Today
    if freq in ("d-0", "d0", "today"):
        sql = f"DATE(s.`{date_col}`) = CURDATE()"
        target = datetime.now().strftime("%Y-%m-%d")
        return sql, True, "Today", target

    # Yesterday / Daily
    if freq in ("yesterday", "daily"):
        sql = f"DATE(s.`{date_col}`) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)"
        target = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        return sql, True, "Yesterday/Daily (1 day ago)", target

    # Hourly
    if freq == "hourly":
        sql = f"s.`{date_col}` >= DATE_SUB(NOW(), INTERVAL 1 HOUR)"
        target_time = datetime.now() - timedelta(hours=1)
        return sql, False, "Hour lookback: last 1 hour", f"Last 1h (since {target_time.strftime('%H:%M:%S')})"

    # Full Sync (Entire history / All data)
    if freq in ("all", "full", "history"):
        sql = "1=1"
        return sql, False, "Full history sync (All records)", "All History"

    # 1. Day-based lookback: d-X, dX, d_X (where X is any positive integer)
    m_day = re.match(r'^d[-_]?(\d+)$', freq)
    if m_day:
        days = int(m_day.group(1))
        sql = f"DATE(s.`{date_col}`) = DATE_SUB(CURDATE(), INTERVAL {days} DAY)"
        target = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        return sql, True, f"Day lookback: {days} day(s) ago", target


    # 2. Minute-based lookback: Xm, Xmin, Xmins, Xminute, Xminutes
    m_min = re.match(r'^(\d+)\s*(m|min|mins|minute|minutes)$', freq)
    if m_min:
        mins = int(m_min.group(1))
        lookback = 2 if mins == 1 else mins
        sql = f"s.`{date_col}` >= DATE_SUB(NOW(), INTERVAL {lookback} MINUTE)"
        target_time = datetime.now() - timedelta(minutes=lookback)
        return sql, False, f"Minute lookback: last {lookback} minute(s)", f"Last {lookback}m (since {target_time.strftime('%H:%M:%S')})"

    # 3. Hour-based lookback: Xh, Xhr, Xhrs, Xhour, Xhours
    m_hr = re.match(r'^(\d+)\s*(h|hr|hrs|hour|hours)$', freq)
    if m_hr:
        hours = int(m_hr.group(1))
        sql = f"s.`{date_col}` >= DATE_SUB(NOW(), INTERVAL {hours} HOUR)"
        target_time = datetime.now() - timedelta(hours=hours)
        return sql, False, f"Hour lookback: last {hours} hour(s)", f"Last {hours}h (since {target_time.strftime('%H:%M:%S')})"

    # Fallback default
    logger.warning(f"Unrecognized frequency '{frequency_str}'. Falling back to d-1 (Yesterday).")
    sql = f"DATE(s.`{date_col}`) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)"
    target = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    return sql, True, "Default fallback (d-1)", target


def verify_schema(source_table, dest_table, source_cols, dest_cols):
    """
    Verifies that source/destination tables and columns exist in the database.
    """
    def parse_db_table(resolved_name):
        parts = [p.replace('`', '').strip() for p in resolved_name.split('.')]
        if len(parts) == 2:
            return parts[0], parts[1]
        return DB_DATA_NAME, parts[0]

    # Verify destination table
    dest_db, dest_tbl = parse_db_table(dest_table)
    dest_query = """
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
    """
    try:
        with DatabaseConnection() as conn:
            cursor = conn.cursor()
            cursor.execute(dest_query, (dest_db, dest_tbl))
            existing_dest_cols = {row[0].lower() for row in cursor.fetchall()}
            cursor.close()
    except Exception as e:
        return False, f"Database check failure on table '{dest_table}': {e}"

    if not existing_dest_cols:
        return False, f"Destination table '{dest_table}' does not exist."

    for d_col in dest_cols:
        if d_col.lower() not in existing_dest_cols:
            return False, f"Destination column '{d_col}' does not exist in table '{dest_table}'."

    # Verify source table
    src_db, src_tbl = parse_db_table(source_table)
    src_query = """
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
    """
    try:
        with DatabaseConnection() as conn:
            cursor = conn.cursor()
            cursor.execute(src_query, (src_db, src_tbl))
            existing_src_cols = {row[0].lower() for row in cursor.fetchall()}
            cursor.close()
    except Exception as e:
        return False, f"Database check failure on table '{source_table}': {e}"

    if not existing_src_cols:
        return False, f"Source table '{source_table}' does not exist."

    for s_col in source_cols:
        if s_col.lower() not in existing_src_cols:
            return False, f"Source column '{s_col}' does not exist in table '{source_table}'."

    return True, None


def process_single_config(config, frequency_filter=None, dry_run=False, is_override=False):
    """
    Processes a single configuration mapping: splits tables and frequencies,
    runs schema checks, executes insert synchronization, and logs history.
    """
    sno = config["id"]
    module_name = config.get("module_name") or "Unknown Module"
    dest_table = config["dest_table_name"]
    dest_table_resolved = get_table_with_db(dest_table, DB_DATA_NAME)
    date_column = config["date_column_name"]
    group_by = config["group_by"]

    # Parse comma-separated source tables
    source_tables_raw = config["source_table_name"]
    source_tables = [t.strip() for t in source_tables_raw.split(",") if t.strip()]

    # Parse comma-separated frequencies (default: d-1)
    frequency_raw = config.get("frequency") or "d-1"
    frequencies = [f.strip() for f in frequency_raw.split(",") if f.strip()]

    dest_cols = [c.strip() for c in config["dest_column_name"].split(",") if c.strip()]
    source_cols = [c.strip() for c in config["source_column_name"].split(",") if c.strip()]

    if len(dest_cols) != len(source_cols):
        logger.error(f"Config #{sno}: Destination and source column counts mismatch.")
        return False

    mapping = list(zip(dest_cols, source_cols))

    # Match frequencies against CLI frequency filter (if provided)
    matched_frequencies = []
    for freq in frequencies:
        if frequency_filter:
            # Match shorthand (e.g. CLI '5m' will run config's '5m' or '5min')
            if frequency_filter.strip().lower() in freq.lower():
                matched_frequencies.append(freq)
        else:
            matched_frequencies.append(freq)

    # Parse existing sync_state from database column
    sync_state_raw = config.get("sync_state") or "{}"
    try:
        sync_state_dict = json.loads(sync_state_raw)
    except Exception:
        sync_state_dict = {}

    # Filter matched frequencies to only those that are due (if not overridden)
    due_frequencies = []
    for freq in matched_frequencies:
        if is_override:
            due_frequencies.append(freq)
            continue

        last_run_str = sync_state_dict.get(freq.strip().lower())
        last_run = None
        if last_run_str:
            try:
                last_run = datetime.strptime(last_run_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass

        if not last_run:
            due_frequencies.append(freq)
            continue

        now = datetime.now()
        freq_clean = freq.strip().lower()

        # Check minutes (Xm)
        m_min = re.match(r'^(\d+)\s*(m|min|mins|minute|minutes)$', freq_clean)
        if m_min:
            mins = int(m_min.group(1))
            elapsed = (now - last_run).total_seconds()
            if elapsed >= (mins * 60 - 5):  # 5 seconds drift buffer
                due_frequencies.append(freq)
            else:
                logger.debug(f"Config #{sno} | Freq: {freq}: Skipped (elapsed {int(elapsed)}s < {mins*60}s).")

        # Check hours (Xh or "hourly")
        elif re.match(r'^(\d+)\s*(h|hr|hrs|hour|hours)$', freq_clean) or freq_clean == "hourly":
            m_hr = re.match(r'^(\d+)\s*(h|hr|hrs|hour|hours)$', freq_clean)
            hours = int(m_hr.group(1)) if m_hr else 1
            elapsed = (now - last_run).total_seconds()
            if elapsed >= (hours * 3600 - 5):
                due_frequencies.append(freq)
            else:
                logger.debug(f"Config #{sno} | Freq: {freq}: Skipped (elapsed {int(elapsed)}s < {hours*3600}s).")

        # Check days (d-X, dX, d_X, today, yesterday)
        else:
            if last_run.date() < now.date():
                due_frequencies.append(freq)
            else:
                logger.debug(f"Config #{sno} | Freq: {freq}: Skipped (already run today).")

    if not due_frequencies:
        logger.debug(f"Config #{sno}: No frequencies are due at this time. Skipping configuration.")
        return True

    logger.info(f"Config #{sno} ({module_name}): Processing {len(source_tables)} source table(s) on due frequencies: {due_frequencies}")

    if not dry_run:
        try:
            with DatabaseConnection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE dashboard_filter_value_sync_map SET last_sync_status = 'RUNNING', last_sync_time = %s WHERE id = %s",
                    (datetime.now(), sno)
                )
                conn.commit()
                cursor.close()
        except Error as e:
            logger.error(f"Config #{sno}: Failed to update status to RUNNING: {e}")

    total_processed = 0
    total_inserted = 0
    run_status = "SUCCESS"
    last_error = None

    # Sync for each matched frequency and source table
    for freq in due_frequencies:
        sql_cond, is_day_based, freq_desc, freq_target = resolve_frequency_condition(freq, date_column)

        # Truncate destination table once before processing source tables for full sync
        if freq.strip().lower() in ("all", "full", "history"):
            if dry_run:
                logger.info(f"[DRY-RUN] Config #{sno}: Truncate destination table {dest_table_resolved}")
            else:
                logger.info(f"Config #{sno}: Truncating destination table {dest_table_resolved} before full sync...")
                try:
                    with DatabaseConnection() as conn:
                        cursor = conn.cursor()
                        cursor.execute(f"TRUNCATE TABLE {dest_table_resolved}")
                        conn.commit()
                        cursor.close()
                    logger.info(f"Config #{sno}: Destination table truncated successfully.")
                except Exception as e:
                    logger.error(f"Config #{sno}: Failed to truncate destination table {dest_table_resolved}: {e}")

        for src_tbl_name in source_tables:
            src_table_resolved = get_table_with_db(src_tbl_name, DB_DATA_NAME)
            logger.info(f"Config #{sno}: Checking {src_table_resolved} -> {dest_table_resolved} | Frequency: {freq} ({freq_desc})")

            # 1. Schema Validation
            valid, schema_err = verify_schema(src_table_resolved, dest_table_resolved, source_cols, dest_cols)
            if not valid:
                logger.error(f"Config #{sno} ({src_tbl_name}): Schema invalid: {schema_err}")
                run_status = "FAILED"
                last_error = schema_err
                if not dry_run:
                    log_execution(sno, freq, src_tbl_name, "FAILED", 0, 0, schema_err)
                continue

            # 2. Count Matching Source Rows
            count_query = f"SELECT COUNT(*) AS total FROM {src_table_resolved} s WHERE {sql_cond}"
            rows_processed = 0
            try:
                with DatabaseConnection() as conn:
                    cursor = conn.cursor(dictionary=True)
                    cursor.execute(count_query)
                    rows_processed = cursor.fetchone()["total"]
                    cursor.close()
            except Exception as e:
                err_msg = f"Failed counting rows on {src_tbl_name}: {e}"
                logger.error(f"Config #{sno}: {err_msg}")
                run_status = "FAILED"
                last_error = err_msg
                if not dry_run:
                    log_execution(sno, freq, src_tbl_name, "FAILED", 0, 0, err_msg)
                continue

            total_processed += rows_processed

            if rows_processed == 0:
                logger.info(f"Config #{sno} ({src_tbl_name}): 0 source rows found matching criteria. Skipping sync execution.")
                if not dry_run:
                    log_execution(sno, freq, src_tbl_name, "SUCCESS", 0, 0, f"No new rows found ({freq_desc})")
                continue

            # 3. Build Dynamic SQL Query
            select_exprs = []
            not_exists_conds = []

            for d_col, s_col in mapping:
                if s_col == date_column and is_day_based:
                    select_exprs.append(f"DATE(s.`{s_col}`) AS `{d_col}`")
                    not_exists_conds.append(f"d.`{d_col}` <=> DATE(s.`{s_col}`)")
                else:
                    select_exprs.append(f"s.`{s_col}` AS `{d_col}`")
                    not_exists_conds.append(f"d.`{d_col}` <=> s.`{s_col}`")

            select_list = ", ".join(select_exprs)
            dest_list = ", ".join([f"`{c}`" for c in dest_cols])
            not_exists_str = " AND ".join(not_exists_conds)

            sync_query = f"""
                INSERT IGNORE INTO {dest_table_resolved} ({dest_list})
                SELECT {select_list}
                FROM {src_table_resolved} s
                WHERE {sql_cond}
                  AND NOT EXISTS (
                      SELECT 1 
                      FROM {dest_table_resolved} d 
                      WHERE {not_exists_str}
                  )
            """

            if group_by:
                group_by_cols = ", ".join([f"s.`{g.strip()}`" for g in group_by.split(",") if g.strip()])
                if group_by_cols:
                    sync_query += f" GROUP BY {group_by_cols}"

            if dry_run:
                logger.info(f"[DRY-RUN] Config #{sno} Query:\n{sync_query}")
                logger.info(f"[DRY-RUN] Config #{sno}: Estimated source rows: {rows_processed}")
                continue

            # 4. Execute Insert Sync
            rows_inserted = 0
            try:
                with DatabaseConnection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(sync_query)
                    rows_inserted = cursor.rowcount
                    conn.commit()
                    cursor.close()
                total_inserted += rows_inserted
                logger.info(f"Config #{sno}: Synced {rows_inserted} new rows from {src_tbl_name} ({freq_desc})")
                log_execution(sno, freq, src_tbl_name, "SUCCESS", rows_processed, rows_inserted, None)
            except Exception as e:
                err_msg = f"Failed executing query for {src_tbl_name}: {e}"
                logger.error(f"Config #{sno}: {err_msg}")
                run_status = "FAILED"
                last_error = err_msg
                if not dry_run:
                    log_execution(sno, freq, src_tbl_name, "FAILED", rows_processed, 0, err_msg)

        # Mark this specific frequency as run for this configuration in the DB column
        if not dry_run:
            sync_state_dict[freq.strip().lower()] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                with DatabaseConnection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "UPDATE dashboard_filter_value_sync_map SET sync_state = %s WHERE id = %s",
                        (json.dumps(sync_state_dict), sno)
                    )
                    conn.commit()
                    cursor.close()
            except Error as e:
                logger.error(f"Config #{sno}: Failed to update sync_state in database: {e}")

    # 5. Write final summarized status to config map table
    if not dry_run:
        try:
            with DatabaseConnection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE dashboard_filter_value_sync_map 
                    SET last_sync_status = %s,
                        rows_processed = %s,
                        rows_inserted = %s,
                        last_sync_time = %s
                    WHERE id = %s
                """,
                    (run_status, total_processed, total_inserted, datetime.now(), sno),
                )
                conn.commit()
                cursor.close()
        except Error as e:
            logger.error(f"Config #{sno}: Failed to write final status back to map: {e}")

    return run_status == "SUCCESS"


def log_execution(map_id, frequency, source_table, status, rows_processed, rows_inserted, error_message):
    """Inserts a run entry in the dashboard_filter_value_sync_log history table."""
    try:
        log_msg = f"Source Table: {source_table} | Frequency: {frequency}"
        if error_message:
            log_msg += f" | Error: {error_message}"
        else:
            log_msg += " | Sync execution completed successfully."

        with DatabaseConnection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO dashboard_filter_value_sync_log 
                  (map_id, run_time, status, rows_processed, rows_inserted, error_message)
                VALUES (%s, %s, %s, %s, %s, %s)
            """,
                (
                    map_id,
                    datetime.now(),
                    status,
                    rows_processed,
                    rows_inserted,
                    log_msg,
                ),
            )
            conn.commit()
            cursor.close()
    except Error as e:
        logger.error(f"Failed to log execution history for config #{map_id}: {e}")


def cleanup_old_db_logs(days_to_keep=7, dry_run=False):
    """Deletes execution logs from the database that are older than days_to_keep days, once per day."""
    import os
    
    # State file to track the last successful cleanup date
    base_dir = os.path.dirname(os.path.abspath(__file__))
    state_file = os.path.join(base_dir, "logs", ".last_cleanup_date")
    today_str = datetime.now().strftime("%Y-%m-%d")

    # If already cleaned up today, skip entirely to save DB resources
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                last_cleanup = f.read().strip()
            if last_cleanup == today_str:
                logger.debug("Database log cleanup already executed today. Skipping.")
                return
        except Exception as e:
            logger.warning(f"Failed to read cleanup state file: {e}")

    if dry_run:
        logger.info(f"[DRY-RUN] Would clean up database execution logs older than {days_to_keep} days.")
        return

    try:
        query = """
            DELETE FROM dashboard_filter_value_sync_log 
            WHERE run_time < DATE_SUB(NOW(), INTERVAL %s DAY)
        """
        with DatabaseConnection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (days_to_keep,))
            conn.commit()
            rows_deleted = cursor.rowcount
            cursor.close()
        
        if rows_deleted > 0:
            logger.info(f"Cleaned up {rows_deleted} old database log history records older than {days_to_keep} days.")
        else:
            logger.debug("Database log cleanup checked. No records older than 7 days found.")

        # Save today's date to state file to prevent another run today
        try:
            os.makedirs(os.path.dirname(state_file), exist_ok=True)
            with open(state_file, "w", encoding="utf-8") as f:
                f.write(today_str)
        except Exception as e:
            logger.error(f"Failed to save cleanup state date: {e}")

    except Error as e:
        logger.error(f"Failed to clean up old database logs: {e}")


def run_synchronization():
    """Main execution runner: Parses parameters, queries configurations, runs tasks in thread pool."""
    parser = argparse.ArgumentParser(description="Advanced Database Filter Synchronization ETL Tool")
    parser.add_argument("--config-id", type=int, help="Filter run to only a specific Configuration ID (SNO)")
    parser.add_argument("--frequency", type=str, help="Filter run to only configs matching this frequency (e.g. 1m, 5m, d-1)")
    parser.add_argument("--dry-run", action="store_true", help="Simulate run without performing updates or writing logs")
    parser.add_argument("--date", type=str, help="Override run date in YYYY-MM-DD format (maps dynamically to d-X)")
    
    # Optional positional arguments for legacy/web UI compatibility
    parser.add_argument("pos_config_id", type=int, nargs="?", help="Position-based Configuration ID (SNO)")
    parser.add_argument("pos_date", type=str, nargs="?", help="Position-based Target Date Override (YYYY-MM-DD)")

    args = parser.parse_args()

    # Resolve arguments (prioritize named over positional)
    config_id = args.config_id if args.config_id is not None else args.pos_config_id
    target_date_str = args.date if args.date is not None else args.pos_date

    logger.info("Starting database filter synchronization job...")
    if args.dry_run:
        logger.info("!!! DRY-RUN MODE ACTIVE: No database changes will be committed !!!")

    # Load active configurations
    configs = []
    try:
        with DatabaseConnection() as conn:
            cursor = conn.cursor(dictionary=True)

            if config_id is not None:
                cursor.execute(
                    "SELECT * FROM dashboard_filter_value_sync_map WHERE is_active = 1 AND id = %s",
                    (config_id,)
                )
            else:
                cursor.execute(
                    "SELECT * FROM dashboard_filter_value_sync_map WHERE is_active = 1"
                )
            configs = cursor.fetchall()
            cursor.close()
    except Error as e:
        logger.critical(f"Failed to retrieve configuration from database: {e}")
        sys.exit(1)

    if not configs:
        logger.info("No active configurations found to process.")
        return

    # Handle legacy --date argument by mapping it to dynamic d-X frequency
    if target_date_str:
        try:
            target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
            days_ago = (datetime.now().date() - target_date).days
            logger.info(f"Custom date override '{target_date_str}' mapped to dynamic frequency: d-{days_ago}")
            for config in configs:
                config["frequency"] = f"d-{days_ago}"
        except ValueError:
            logger.error(f"Invalid date format: {target_date_str}. Please use YYYY-MM-DD.")
            sys.exit(1)

    # Determine if this is a manual override execution (filtered/triggered via command line)
    is_override = (args.frequency is not None) or (config_id is not None) or (target_date_str is not None)
    
    if is_override:
        freq_filter = args.frequency
    else:
        # If no command line override filters are given, run in automatic smart scheduling mode
        freq_filter = None

    logger.info(f"Processing {len(configs)} configuration(s) using up to {MAX_WORKERS} threads. Mode: {'Manual Override' if is_override else 'Smart Auto-Schedule'}")

    success_count = 0
    total_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for config in configs:
            # Submit to thread pool
            futures[executor.submit(process_single_config, config, freq_filter, args.dry_run, is_override)] = config

        for future in as_completed(futures):
            config = futures[future]
            sno = config["id"]
            total_count += 1
            try:
                success = future.result()
                if success:
                    success_count += 1
            except Exception as e:
                logger.error(f"Exception raised while executing config #{sno}: {e}")

    logger.info(f"Database filter synchronization job completed. {success_count}/{total_count} configurations processed successfully.")

    # Automatically clean up database execution logs older than 7 days (runs once per calendar day internally)
    cleanup_old_db_logs(days_to_keep=7, dry_run=args.dry_run)


if __name__ == "__main__":
    run_synchronization()
