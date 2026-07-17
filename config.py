import os
import sys

# Custom parser to load .env file to support clean local configurations without external dependencies
def load_env_file():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(base_dir, ".env")
    if os.path.exists(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip()
                    if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                        val = val[1:-1]
                    if key:
                        os.environ[key] = val
        except Exception as e:
            print(f"Warning: Failed to parse .env file: {e}", file=sys.stderr)

# Custom parser to load config.json if present
def load_json_file():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(base_dir, "config.json")
    if os.path.exists(json_path):
        import json
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                for key, val in data.items():
                    os.environ[key] = str(val)
        except Exception as e:
            print(f"Warning: Failed to parse config.json file: {e}", file=sys.stderr)

# Load configurations
load_env_file()
load_json_file()

# Database Connection Configuration
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_USER = os.environ.get("DB_USER", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "admin@321")
DB_NAME = os.environ.get("DB_NAME", "pview_config_v5")
DB_DATA_NAME = os.environ.get("DB_DATA_NAME", "pview")
DB_PORT = int(os.environ.get("DB_PORT", 3306))

# Concurrency & Scheduling Defaults
MAX_WORKERS = max(1, int(os.environ.get("MAX_WORKERS", 4)))
DEFAULT_FREQUENCY = os.environ.get("DEFAULT_FREQUENCY", "d-1")

# Logger settings
LOG_FILE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "logs", "sync.log"
)
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

