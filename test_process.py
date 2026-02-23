import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from process import run_processing_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger("test_process")

def test_pipeline():
    print("--- Starting Local Processing Test ---")
    
    run_ts = datetime.now(timezone.utc)
    
    try:
        results = run_processing_pipeline(lookback_hours=48, run_ts=run_ts)
        
        print("\n--- Processing Results ---")
        if results["status"] == "success":
            print(f"Raw Records Found:    {results['raw_rows']}")
            print(f"Cleaned Records:      {results['clean_rows']}")
            print(f"Hourly Average Rows:  {results['hourly_rows']}")
            print(f"Daily Average Rows:   {results['daily_rows']}")
            print(f"Latest Reading Rows:  {results['latest_rows']}")
            print(f"City Summary Rows:    {results['city_summary_rows']}")
            
            processed_base = Path("data/processed")
            if processed_base.exists():
                print(f"\n✅ SUCCESS: Processed files created in {processed_base.absolute()}")
            else:
                print("\n⚠️ WARNING: Processing reported success but directory not found.")
        else:
            print(f"❌ FAILED: Pipeline returned status: {results['status']}")
            
    except Exception as e:
        print(f"\n❌ ERROR: Test failed with: {e}")
        sys.exit(1)

if __name__ == "__main__":
    test_pipeline()