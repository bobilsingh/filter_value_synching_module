#!/bin/bash

# Navigate to the directory where this script is located
cd "$(dirname "$0")"

echo "=== DB Filter Value Synchronization ==="

# 1. Verify Python 3 is installed
if ! command -v python3 &>/dev/null; then
    echo "Error: python3 is not installed or not in PATH."
    exit 1
fi

# 2. Check if mysql-connector-python is installed, try to auto-install if missing
if ! python3 -c "import mysql.connector" &>/dev/null; then
    echo "Warning: mysql-connector-python is not installed."
    echo "Attempting to install it locally..."
    python3 -m pip install --user mysql-connector-python
    
    # Verify installation success
    if ! python3 -c "import mysql.connector" &>/dev/null; then
        echo "Error: Failed to install mysql-connector-python automatically."
        echo "Please install it manually using: pip install mysql-connector-python"
        exit 1
    fi
    echo "mysql-connector-python installed successfully!"
fi

# 3. Run the synchronization script passing any arguments.
# Supported arguments:
#   --config-id <id>     Run only a specific Configuration SNO
#   --frequency <freq>   Run configs matching frequency (e.g. 1m, 5m, d-1, d-15)
#   --dry-run            Simulate runs and log queries without modifying database
#   --date <YYYY-MM-DD>  Override run date (maps dynamically to d-X)
python3 sync_filters.py "$@"
