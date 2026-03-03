import schedule
import time
from pipelines.orchestrator import run_pipeline

def job():
    print("Running ETL pipeline...")
    run_pipeline()

# Run every day at midnight
schedule.every().day.at("00:00").do(job)

print("Scheduler started...")
while True:
    schedule.run_pending()
    time.sleep(60)