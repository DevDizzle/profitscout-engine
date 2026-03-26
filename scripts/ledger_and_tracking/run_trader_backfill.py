import requests
import datetime
import time

def main():
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=40)
    
    current_date = start_date
    url = "https://forward-paper-trader-hrhjaecvhq-uc.a.run.app/"
    
    print(f"Backfilling from {start_date} to {end_date}")
    
    while current_date <= end_date:
        if current_date.weekday() < 5:  # Monday to Friday
            date_str = current_date.strftime("%Y-%m-%d")
            print(f"Triggering paper trader for scan_date: {date_str}")
            
            try:
                resp = requests.post(
                    url,
                    json={"target_date": date_str},
                    headers={"Content-Type": "application/json"}
                )
                if resp.status_code == 200:
                    print(f"  Success: {resp.json().get('message')}")
                else:
                    print(f"  Failed: {resp.status_code} - {resp.text}")
            except Exception as e:
                print(f"  Error triggering {date_str}: {e}")
                
            time.sleep(1) # Small pause between requests
        current_date += datetime.timedelta(days=1)

if __name__ == "__main__":
    main()
