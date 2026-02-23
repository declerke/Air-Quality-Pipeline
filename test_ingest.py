import logging
import os
from datetime import datetime, timezone
from pathlib import Path
import ingest

# 1. Setup logging to see what's happening
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 2. Override the landing directory for local Windows testing
# This ensures we don't try to write to /opt/airflow/...
os.environ["AQ_DATA_LANDING"] = str(Path.cwd() / "data" / "landing")

def run_local_test():
    print("--- Starting Local Ingestion Test (Kenya) ---")
    
    # We'll use a fixed timestamp for the test
    now = datetime.now(timezone.utc)
    
    try:
        # Run the ingestion for Kenya
        result = ingest.ingest_country("KE", now)
        
        print("\n--- Test Results ---")
        print(f"Country:   {result['country']}")
        print(f"Locations: {result['locations']}")
        print(f"Records:   {result['records']}")
        print(f"File Path: {result['file']}")
        
        if result['file'] and os.path.exists(result['file']):
            print("\n✅ SUCCESS: Parquet file created successfully!")
        else:
            print("\n⚠️ WARNING: Process finished but no file was created. Check logs.")
            
    except Exception as e:
        print(f"\n❌ ERROR: Test failed with: {str(e)}")

if __name__ == "__main__":
    run_local_test()