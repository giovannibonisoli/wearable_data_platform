"""
FITBIT INTRADAY - CONTINUOUS BACKGROUND LOOP

This script continuously collects intraday Fitbit data for multiple users in the background.

MAIN FUNCTIONALITY:
- Runs continuously in a loop until it reaches datetime.now().date()
- If a user hits rate limit (429), skip to next user
- If all users hit rate limit, sleep for 10 minutes
- Uses database checkpoint (no individual metric checkpoints)
- Automatically updates checkpoint using db.update_intraday_checkpoint()

Key variables to modify:
    BACKFILL_START_DATE = "2025-01-20"  # First day to collect (inclusive)
"""

from base64 import b64encode
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta

import sys
import os
import json
import time
import logging

from db import DatabaseManager
from auth import refresh_tokens, get_device_info

# Logs configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("logs/fitbit_intraday_debug.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

# === BACKFILL CONFIGURATION ===
BACKFILL_START_DATE = "2025-01-24"  # First day to collect (inclusive)
SLEEP_ON_RATE_LIMIT = 600  # 10 minutes in seconds


def request_with_rate_limit(url, access_token, max_retries=3):
    """Make request with rate limit handling. Returns (data, hit_limit)"""
    headers = {"Authorization": f"Bearer {access_token}"}
    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, headers=headers)
        limit = resp.headers.get('Fitbit-Rate-Limit-Limit')
        remaining = resp.headers.get('Fitbit-Rate-Limit-Remaining')
        reset = resp.headers.get('Fitbit-Rate-Limit-Reset')
        logger.debug(f"Rate-limit: {remaining}/{limit}, resets in {reset}s")

        if resp.status_code == 429:
            logger.warning(f"429 Rate limit hit (attempt {attempt})")
            return None, True  # Signal rate limit hit
        
        if resp.status_code == 401:
            raise requests.exceptions.HTTPError("401 Unauthorized", response=resp)

        resp.raise_for_status()
        return resp.json(), False

    return None, True  # Exceeded retries


def get_intraday_data(access_token, email_address, date_str, last_synch_date):
    """Collect intraday data for a specific date. Returns (success, hit_rate_limit)"""
    db = DatabaseManager()
    if not db.connect():
        logger.error("Failed to connect to database")
        return False, False
    
    try:
        logger.info(f"Collecting intraday data for {email_address['address_name']} on {date_str}")
        total_points = 0
        detail_level = "1min"
        hit_rate_limit = False

        
        metrics = [
            ('heart_rate', f"https://api.fitbit.com/1/user/-/activities/heart/date/{date_str}/1d/{detail_level}.json", 'activities-heart-intraday'),
            ('steps', f"https://api.fitbit.com/1/user/-/activities/steps/date/{date_str}/1d/{detail_level}.json", 'activities-steps-intraday'),
            ('calories', f"https://api.fitbit.com/1/user/-/activities/calories/date/{date_str}/1d/{detail_level}.json", 'activities-calories-intraday'),
            ('distance', f"https://api.fitbit.com/1/user/-/activities/distance/date/{date_str}/1d/{detail_level}.json", 'activities-distance-intraday'),
        ]

        data_points = {}
        for data_type, url, key in metrics:
            data, rate_limited = request_with_rate_limit(url, access_token)
            
            if rate_limited:
                logger.warning(f"Rate limit hit for {email_address['address_name']} on {data_type}")
                return False, True
            
            if data and key in data:
                intraday_data = data[key]
                dataset = intraday_data.get('dataset', [])
                

                for point in dataset:
                    time_str = point.get('time')
                    value = point.get('value')
                    if time_str and value is not None:
                        timestamp = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
                        last_synch_date = last_synch_date.replace(tzinfo=timestamp.tzinfo)

                        if timestamp not in data_points:
                            data_points[timestamp] = {}

                        data_points[timestamp][data_type] = value

                        # db.insert_intraday_metric(email_address['id'], timestamp, data_type=data_type, value=value)
                        total_points += 1


        timestamps = [timestamp for timestamp in list(data_points.keys()) if timestamp <= last_synch_date]
        timestamps.sort()

        for timestamp in timestamps:

            values = data_points[timestamp]
            if not ('heart_rate' not in values and values['steps'] == 0 and values['distance'] == 0):
                db.insert_intraday_metric(email_address['id'], timestamp, data_type='heart_rate', value=values['heart_rate'])
                db.insert_intraday_metric(email_address['id'], timestamp, data_type='steps', value=values['steps'])
                db.insert_intraday_metric(email_address['id'], timestamp, data_type='distance', value=values['distance'])
                db.insert_intraday_metric(email_address['id'], timestamp, data_type='calories', value=values['calories'])

            else:
                print(f"Empty point or checkpoint reached for timestamp {timestamp}")

            db.update_intraday_checkpoint(email_address['id'], timestamp)


        # Update checkpoint in database
        if total_points > 0:
            logger.info(f"✓ Collected {total_points} points for {email_address['address_name']} on {date_str}")
            return True, False
        else:
            logger.warning(f"No data collected for {email_address['address_name']} on {date_str}")
            return False, False
            
    except requests.exceptions.HTTPError as e:
        if bool(hasattr(e, 'response') and e.response.status_code == 401):
            logger.error(f"Authentication error (401) for {email_address['address_name']}")
            raise
        logger.error(f"HTTP error: {e}")
        return False, False
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return False, False
    finally:
        db.close()


def process_all_emails_continuous():
    """Main continuous loop that processes all emails until current date"""
    db = DatabaseManager()
    if not db.connect():
        logger.error("Failed to connect to database")
        return
    
    email_addresses = db.get_all_emails()
    db.close()
    
    if len(email_addresses) == 0:
        logger.error("No email addresses found")
        return

    start_date = datetime.strptime(BACKFILL_START_DATE, "%Y-%m-%d").date()
    
    logger.info(f"Starting continuous processing from {start_date} to current date")

    
    while True:
        today = datetime.now().date()
        all_emails_rate_limited = True
        any_email_processed = False
        
        for email_address in email_addresses:
            db = DatabaseManager()
            if not db.connect():
                continue
                
            # Get checkpoint from database
            intraday_checkpoint = db.get_intraday_checkpoint(email_address['id'])
  
            if intraday_checkpoint:
                current_date = intraday_checkpoint + timedelta(minutes=1)
                print("GOT CHECKPOINT:", current_date)
            else:
                current_date = start_date
                print("NEW START DATE:", current_date)
                db.update_intraday_checkpoint(email_address['id'], start_date)

            access_token, refresh_token = db.get_email_tokens(email_address['id'])
            db.close()

            if not access_token or not refresh_token:
                logger.warning(f"No valid tokens for {email_address['address_name']}")
                continue
            
            try:

                db = DatabaseManager()
                if not db.connect():
                    continue

                last_synch_date = db.get_last_synch(email_address['id'])

                if current_date >= last_synch_date:
                
                    device_data = get_device_info(access_token)
                    new_last_synch_date = device_data['lastSyncTime']
                    new_last_synch_date = new_last_synch_date.replace(tzinfo=current_date.tzinfo)

                    logger.info(f"Checking last synch date for email {email_address['address_name']}")
                    
                    if new_last_synch_date == last_synch_date:
                        logger.info(f"Email {email_address['address_name']} is up to date (last: {new_last_synch_date})")
                    else:
                        logger.info(f"Updating last synch date for {email_address['address_name']} to:{new_last_synch_date}")
                        db.update_last_synch(email_address['id'], new_last_synch_date.strftime('%Y-%m-%d %H:%M:%S'))
                        last_synch_date = new_last_synch_date

                db.close()

                # Check if email is up to date
                if current_date >= last_synch_date:
                    continue

                # Process this email's next day
                date_str = current_date.strftime('%Y-%m-%d')
            
                success, hit_rate_limit = get_intraday_data(access_token, email_address, date_str, last_synch_date)
                
                if hit_rate_limit:
                    logger.warning(f"Rate limit hit for {email_address['address_name']}, skipping to next user")
                    continue  # Skip to next email
                else:
                    all_emails_rate_limited = False
                    any_email_processed = True
                
                time.sleep(1)  # Small delay between requests
                
            except requests.exceptions.HTTPError as e:
                if hasattr(e, 'response') and e.response.status_code == 401:
                    logger.warning(f"Token expired for {email_address['address_name']}, attempting refresh...")
                    new_access_token, new_refresh_token = refresh_tokens(refresh_token)
                    if new_access_token and new_refresh_token:
                        db = DatabaseManager()
                        if db.connect():
                            db.update_email_tokens(email_address['id'], new_access_token, new_refresh_token)
                            db.close()
                        # Retry with new token
                        try:
                            success, hit_rate_limit = get_intraday_data(new_access_token, email_address, date_str, last_synch_date)
                            if not hit_rate_limit:
                                all_emails_rate_limited = False
                                any_email_processed = True
                        except Exception as e2:
                            logger.error(f"Error after token refresh: {e2}")
                    else:
                        logger.error(f"Could not refresh token for {email_address['address_name']}")
                else:
                    all_emails_rate_limited = False
            except Exception as e:
                logger.error(f"Unexpected error processing {email_address['address_name']}: {e}", exc_info=True)
                all_emails_rate_limited = False
        
        # Check if all emails hit rate limit
        if all_emails_rate_limited and any_email_processed:
            logger.info(f"All emails hit rate limit. Sleeping for {SLEEP_ON_RATE_LIMIT/60} minutes...")
            time.sleep(SLEEP_ON_RATE_LIMIT)
        elif not any_email_processed:
            # All emails are up to date, check again in 10 minutes
            logger.info("All emails up to date. Sleeping for 10 minutes before checking again...")
            time.sleep(SLEEP_ON_RATE_LIMIT)
        else:
            # Continue processing
            time.sleep(2)

        db = DatabaseManager()
    if not db.connect():
        logger.error("Failed to connect to database")
        return
    
    email_addresses = db.get_all_emails()
    db.close()


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    logger.info("=== START OF FITBIT INTRADAY CONTINUOUS LOOP ===")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Working directory: {os.getcwd()}")
    
    try:
        process_all_emails_continuous()
    except KeyboardInterrupt:
        logger.info("\n=== STOPPED BY USER (Ctrl+C) ===")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)