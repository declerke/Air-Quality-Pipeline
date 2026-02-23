import os
import logging
import sys
from datetime import datetime, timezone

os.environ["AQ_POSTGRES_HOST"] = "localhost"
os.environ["AQ_POSTGRES_PORT"] = "5433"
os.environ["AQ_POSTGRES_USER"] = "aquser"
os.environ["AQ_POSTGRES_PASSWORD"] = "aqpassword"
os.environ["AQ_POSTGRES_DB"] = "airquality"

from load_postgres import run_full_load, get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger("test_load")

def test_loading():
    print("--- Starting Local Loading Test ---")
    
    try:
        conn = get_connection()
        print("✅ Database connection established via localhost:5433.")
        conn.close()
    except Exception as e:
        print(f"❌ Connection FAILED: {e}")
        print("\nTroubleshooting tips:")
        print("1. Ensure 'docker ps' shows the postgres-data container is Up.")
        print("2. Confirm port 5433 is mapped in your docker-compose.yml.")
        sys.exit(1)

    try:
        results = run_full_load()
        
        print("\n--- Loading Results ---")
        for table, count in results.items():
            print(f"{table.replace('_', ' ').title()}: {count} rows upserted")
            
        print("\n✅ SUCCESS: Data loaded into PostgreSQL.")
    except Exception as e:
        print(f"\n❌ ERROR: Loading failed with: {e}")
        sys.exit(1)

if __name__ == "__main__":
    test_loading()