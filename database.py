import mysql.connector
from mysql.connector import Error
from mysql.connector.pooling import MySQLConnectionPool
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, MAX_WORKERS
from logger import logger

# Initialize thread-safe connection pool
pool_size = max(5, MAX_WORKERS + 2)
try:
    db_pool = MySQLConnectionPool(
        pool_name="sync_pool",
        pool_size=pool_size,
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
    )
    logger.info(f"Initialized MySQL connection pool 'sync_pool' with size {pool_size}")
except Error as e:
    logger.critical(f"Failed to initialize MySQL connection pool: {e}")
    db_pool = None


class DatabaseConnection:
    """Context manager for managing MySQL database connections, using connection pooling if available."""

    def __init__(self):
        self.conn = None

    def __enter__(self):
        try:
            if db_pool:
                self.conn = db_pool.get_connection()
                logger.debug("Acquired connection from pool.")
            else:
                self.conn = mysql.connector.connect(
                    host=DB_HOST,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    database=DB_NAME,
                    port=DB_PORT,
                )
                logger.debug("Acquired direct connection (pooling inactive).")
            return self.conn
        except Error as e:
            logger.error(f"Failed to connect to MySQL database: {e}")
            raise e

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            try:
                self.conn.close()
                logger.debug("Released connection (returned to pool or closed).")
            except Error as e:
                logger.warning(f"Error closing connection: {e}")

def execute_query(query, params=None, is_select=True):
    """Executes a single SQL query and returns results or row count.

    Args:
        query (str): The SQL statement to run.
        params (tuple, dict, optional): Parameters to pass to the query.
        is_select (bool): True if returning query results; False for modifications.

    Returns:
        list of dicts if is_select is True, otherwise returns lastrowid or rowcount.
    """
    try:
        with DatabaseConnection() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query, params or ())

            if is_select:
                results = cursor.fetchall()
                cursor.close()
                return results
            else:
                conn.commit()
                rowcount = cursor.rowcount
                cursor.close()
                return rowcount
    except Error as e:
        logger.error(f"SQL Execution Error: {e}\nQuery: {query}\nParams: {params}")
        raise e
