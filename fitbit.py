"""
FITBIT DAILY SUMMARY COLLECTOR

Runs continuously in background.
For each email: fetch daily summary from checkpoint date up to yesterday.
Uses one checkpoint per email for summaries only.
Only sleeps when ALL emails are rate-limited.
"""

import os
import time
import logging
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

from db import DatabaseManager
from auth import refresh_tokens

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("logs/fitbit_summary.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

def fetch_endpoint(url, headers, optional=False):
    """Fetch data from endpoint. Returns (data, rate_limited)."""
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json(), False
    elif resp.status_code == 429:
        return None, True
    elif optional:
        return None, False
    else:
        resp.raise_for_status()
        return None, False

def fetch_daily_summary(access_token, email_id, name, date_obj, db):
    """Fetch and store daily summary. Returns (success, rate_limited)."""
    date_str = date_obj.strftime("%Y-%m-%d")
    headers = {"Authorization": f"Bearer {access_token}"}
    
    # Define all endpoints and their data extractors
    endpoints = [
        (f"https://api.fitbit.com/1/user/-/activities/date/{date_str}.json", False, 
         lambda d: {
             'steps': d.get('summary', {}).get('steps', 0),
             'distance': d.get('summary', {}).get('distances', [{}])[0].get('distance', 0),
             'calories': d.get('summary', {}).get('caloriesOut', 0),
             'floors': d.get('summary', {}).get('floors', 0),
             'elevation': d.get('summary', {}).get('elevation', 0),
             'active_minutes': d.get('summary', {}).get('veryActiveMinutes', 0),
             'sedentary_minutes': d.get('summary', {}).get('sedentaryMinutes', 0)
         }),
        (f"https://api.fitbit.com/1/user/-/activities/heart/date/{date_str}/1d.json", False,
         lambda d: {'heart_rate': d.get('activities-heart', [{}])[0].get('value', {}).get('restingHeartRate', 0)}),
        (f"https://api.fitbit.com/1.2/user/-/sleep/date/{date_str}.json", False,
         lambda d: {'sleep_minutes': sum(log.get('minutesAsleep', 0) for log in d.get('sleep', []))}),
        (f"https://api.fitbit.com/1/user/-/foods/log/date/{date_str}.json", False,
         lambda d: {'nutrition_calories': d.get('summary', {}).get('calories', 0)}),
        (f"https://api.fitbit.com/1/user/-/foods/log/water/date/{date_str}.json", False,
         lambda d: {'water': d.get('summary', {}).get('water', 0)}),
        (f"https://api.fitbit.com/1/user/-/spo2/date/{date_str}.json", True,
         lambda d: {'spo2': float(d.get('value', {}).get('avg', 0) if isinstance(d.get('value'), dict) else d.get('value', 0))}),
        (f"https://api.fitbit.com/1/user/-/br/date/{date_str}.json", True,
         lambda d: {'respiratory_rate': float(d.get('value', {}).get('breathingRate', 0) if isinstance(d.get('value'), dict) else d.get('value', 0))}),
        (f"https://api.fitbit.com/1/user/-/temp/core/date/{date_str}.json", True,
         lambda d: {'temperature': d.get('value', 0)})
    ]
    
    data = {k: 0 for k in ['steps', 'distance', 'calories', 'floors', 'elevation', 'active_minutes', 
                            'sedentary_minutes', 'heart_rate', 'sleep_minutes', 'nutrition_calories', 
                            'water', 'spo2', 'respiratory_rate', 'temperature']}
    
    try:
        for url, optional, extractor in endpoints:
            response_data, rate_limited = fetch_endpoint(url, headers, optional)
            if rate_limited:
                return False, True
            if response_data:
                data.update(extractor(response_data))
        
        db.insert_daily_summary(email_id=email_id, date=date_str, **data)
        logger.info(f"Daily summary collected for {name} on {date_str}")
        return True, False
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            raise
        if e.response.status_code == 429:
            return False, True
        logger.error(f"HTTP error fetching summary for {name} on {date_str}: {e}")
        return False, False
    except Exception as e:
        logger.error(f"Unexpected error fetching summary for {name} on {date_str}: {e}")
        return False, False

def process_email_summary(email_record, db):
    """
    Process daily summaries for one email.
    Returns: ('success'|'rate_limited'|'error')
    """
    email_id = email_record['id']
    name = email_record['address_name']

    logger.info(f"Processing daily summary for email {name}")
    access_token, refresh_token = db.get_email_tokens(email_id)
    if not access_token or not refresh_token:
        logger.warning(f"No tokens for {name}")
        return 'error'
        
    last_date = db.get_daily_summary_checkpoint(email_id)
    if last_date:
        start_date = last_date + timedelta(days=1)
    else:
        start_date = datetime(2025,1,25).date()
    end_date = (datetime.now().date() - timedelta(days=1))

    if start_date > end_date:
        logger.info(f"{name} is up to date for summaries")
        return 'success'
    
    current_date = start_date
    
    while current_date <= end_date:
        try:
            success, rate_limited = fetch_daily_summary(access_token, email_id, name, current_date, db)
            
            if rate_limited:
                logger.info(f"Rate limit reached for {name} on {current_date}. Skipping to next email.")
                return 'rate_limited'
            
            if not success:
                # Other error, skip this date and continue
                logger.warning(f"Failed to fetch summary for {name} on {current_date}, continuing...")
                current_date += timedelta(days=1)
                continue
                
            current_date += timedelta(days=1)
            time.sleep(1)  # minimal sleep to buffer
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.warning(f"Token expired for {name}, refreshing …")
                new_access, new_refresh = refresh_tokens(refresh_token)
                if new_access and new_refresh:
                    db.update_email_tokens(email_id, new_access, new_refresh)
                    access_token = new_access
                    refresh_token = new_refresh
                    logger.info(f"Token refreshed for {name}")
                    continue  # retry same date
                else:
                    logger.error(f"Failed token refresh for {name}")
                    return 'error'
            else:
                logger.error(f"HTTP error for {name} on {current_date}: {e}")
                return 'error'
        except Exception as e:
            logger.error(f"Unexpected error for {name} on {current_date}: {e}")
            return 'error'
            
    logger.info(f"Completed summaries for {name} up to {end_date}")
    return 'success'

def main_loop():
    logger.info("=== DAILY SUMMARY COLLECTOR STARTED ===")
    while True:
        try:
            db = DatabaseManager()
            if not db.connect():
                logger.error("DB connection failed")
                time.sleep(60)
                continue

            emails = db.get_all_emails()
            if not emails:
                logger.warning("No emails found")
                db.close()
                time.sleep(60)
                continue

            results = {
                'success': 0,
                'rate_limited': 0,
                'error': 0
            }
            
            for email in emails:
                result = process_email_summary(email, db)
                results[result] += 1
                
            db.close()

            # Log summary
            logger.info(f"Cycle complete: {results['success']} successful, "
                       f"{results['rate_limited']} rate-limited, {results['error']} errors")

            # Only sleep if ALL emails are rate-limited
            if results['rate_limited'] == len(emails) and results['rate_limited'] > 0:
                sleep_interval = 10
                logger.info(f"ALL emails are rate-limited. Sleeping {sleep_interval} minutes before retry.")
                time.sleep(sleep_interval*60)
            else:
                # At least one email processed successfully or had errors
                logger.info("At least one email processed. Sleeping 1 hour before next cycle.")
                time.sleep(3600)
                
        except KeyboardInterrupt:
            logger.info("=== STOPPED BY USER ===")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
            time.sleep(60)

if __name__ == "__main__":
    main_loop()