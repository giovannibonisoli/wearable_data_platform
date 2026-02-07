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
    BACKFILL_START_DATE = "2025-01-24"  # First day to collect (inclusive)
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

from database import Database, ConnectionManager, DeviceRepository, MetricsRepository
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
BACKFILL_START_DATE = "2025-11-18"  # First day to collect (inclusive)
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


def get_intraday_data(access_token, device, date_str, last_synch_date, device_repo, metrics_repo):
    """Collect intraday data for a specific date. Returns (success, hit_rate_limit)"""
    
    try:
        logger.info(f"Collecting intraday data for {device.email_address} on {date_str}")
        total_points = 0
        detail_level = "1min"
        hit_rate_limit = False

        
        metrics = [
            ('heart_rate', f"https://api.fitbit.com/1/user/-/activities/heart/date/{date_str}/1d/{detail_level}.json", 'activities-heart-intraday'),
            ('steps', f"https://api.fitbit.com/1/user/-/activities/steps/date/{date_str}/1d/{detail_level}.json", 'activities-steps-intraday'),
            ('calories', f"https://api.fitbit.com/1/user/-/activities/calories/date/{date_str}/1d/{detail_level}.json", 'activities-calories-intraday'),
            ('distance', f"https://api.fitbit.com/1/user/-/activities/distance/date/{date_str}/1d/{detail_level}.json", 'activities-distance-intraday'),
            ('floors', f"https://api.fitbit.com/1/user/-/activities/floors/date/{date_str}/1d/{detail_level}.json", 'activities-floors-intraday'),
            ('elevation', f"https://api.fitbit.com/1/user/-/activities/elevation/date/{date_str}/1d/{detail_level}.json", 'activities-elevation-intraday'),
        ]

        data_points = {}
        for data_type, url, key in metrics:
            data, rate_limited = request_with_rate_limit(url, access_token)
            
            if rate_limited:
                logger.warning(f"Rate limit hit for {device.email_address} on {data_type}")
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

            # if db.check_intraday_timestamp(email_address['id'], timestamp):
                metrics_repo.insert_intraday_metric(device.id, timestamp, data_type='heart_rate', value=values.get('heart_rate', None))
                metrics_repo.insert_intraday_metric(device.id, timestamp, data_type='steps', value=values['steps'])
                metrics_repo.insert_intraday_metric(device.id, timestamp, data_type='distance', value=values['distance'])
                metrics_repo.insert_intraday_metric(device.id, timestamp, data_type='calories', value=values['calories'])

                metrics_repo.insert_intraday_metric(device.id, timestamp, data_type='floors', value=values['floors'])
                metrics_repo.insert_intraday_metric(device.id, timestamp, data_type='elevation', value=values['elevation'])
            else:
                print(f"Empty point or checkpoint reached for timestamp {timestamp}")

            device_repo.update_intraday_checkpoint(device.id, timestamp)


        # Update checkpoint in database
        if total_points > 0:
            logger.info(f"✓ Collected {total_points} points for {device.email_address} on {date_str}")
            return True, False
        else:
            logger.warning(f"No data collected for {device.email_address} on {date_str}")
            return False, False
            
    except requests.exceptions.HTTPError as e:
        if bool(hasattr(e, 'response') and e.response.status_code == 401):
            logger.error(f"Authentication error (401) for {device.email_address}")
            raise
        logger.error(f"HTTP error: {e}")
        return False, False
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return False, False


def process_all_devices_continuous():
    """Main continuous loop that processes all devices until current date"""


    with ConnectionManager() as conn:
        device_repo = DeviceRepository(conn)
        metrics_repo = MetricsRepository(conn)
    
        devices = device_repo.get_all_authorized()
    
        if len(devices) == 0:
            logger.error("No devices found")
            return

        start_date = datetime.strptime(BACKFILL_START_DATE, "%Y-%m-%d").date()
        logger.info(f"Starting continuous processing from {start_date} to current date")

        while True:
            today = datetime.now().date()
            all_devices_rate_limited = True
            any_device_processed = False
            
            for device in devices: 
                # Get checkpoint from database
                intraday_checkpoint = device.intraday_checkpoint
    
                if intraday_checkpoint:
                    current_date = intraday_checkpoint + timedelta(minutes=1)
                    print("GOT CHECKPOINT:", current_date)
                else:
                    current_date = start_date
                    print("NEW START DATE:", current_date)
                    device_repo.update_intraday_checkpoint(device.id, start_date)

                access_token, refresh_token = device_repo.get_tokens(device.id)

                if not access_token or not refresh_token:
                    logger.warning(f"No valid tokens for {device.email_address}")
                    continue
                
                try:

                    last_synch_date = device.last_synch

                    if current_date >= last_synch_date:
                    
                        device_data = get_device_info(access_token)
                        new_last_synch_date = device_data['lastSyncTime']
                        new_last_synch_date = new_last_synch_date.replace(tzinfo=current_date.tzinfo)

                        logger.info(f"Checking last synch date for device {device.id} with email {device.email_address}")
                        
                        if new_last_synch_date == last_synch_date:
                            logger.info(f"Device {device.id} with email {device.email_address} is up to date (last: {new_last_synch_date})")
                        else:
                            logger.info(f"Updating last synch date for {device.email_address} to: {new_last_synch_date}")
                            device_repo.update_last_synch(device.id, new_last_synch_date.strftime('%Y-%m-%d %H:%M:%S'))
                            last_synch_date = new_last_synch_date

                    # Check if email is up to date
                    if current_date >= last_synch_date:
                        continue

                    # Process this email's next day
                    date_str = current_date.strftime('%Y-%m-%d')
                
                    success, hit_rate_limit = get_intraday_data(access_token, device, date_str, last_synch_date, device_repo, metrics_repo)
                    
                    if hit_rate_limit:
                        logger.warning(f"Rate limit hit for {device.email_address}, skipping to next user")
                        continue  # Skip to next email
                    else:
                        all_devices_rate_limited = False
                        any_device_processed = True
                    
                    time.sleep(1)  # Small delay between requests
                    
                except requests.exceptions.HTTPError as e:
                    if hasattr(e, 'response') and e.response.status_code == 401:
                        logger.warning(f"Token expired for {device.email_address}, attempting refresh...")
                        new_access_token, new_refresh_token = refresh_tokens(refresh_token)
                        if new_access_token and new_refresh_token:

                            device_repo.update_tokens(device.id, new_access_token, new_refresh_token)
                            # Retry with new token
                            try:
                                success, hit_rate_limit = get_intraday_data(new_access_token, device, date_str, last_synch_date, device_repo, metrics_repo)
                                if not hit_rate_limit:
                                    all_devices_rate_limited = False
                                    any_device_processed = True
                            except Exception as e2:
                                logger.error(f"Error after token refresh: {e2}")
                        else:
                            logger.error(f"Could not refresh token for {device.email_address}")
                    else:
                        all_devices_rate_limited = False
                except Exception as e:
                    logger.error(f"Unexpected error processing {device.email_address}: {e}", exc_info=True)
                    all_devices_rate_limited = False
            
            # Check if all devices hit rate limit
            if all_devices_rate_limited and any_device_processed:
                logger.info(f"All devices hit rate limit. Sleeping for {SLEEP_ON_RATE_LIMIT/60} minutes...")
                time.sleep(SLEEP_ON_RATE_LIMIT)

            elif not any_device_processed:
                # All devices are up to date, check again in 10 minutes
                logger.info("All devices up to date. Sleeping for 10 minutes before checking again...")
                time.sleep(SLEEP_ON_RATE_LIMIT)
            else:
                # Continue processing
                time.sleep(2)


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    logger.info("=== START OF FITBIT INTRADAY CONTINUOUS LOOP ===")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Working directory: {os.getcwd()}")
    
    try:
        process_all_devices_continuous()
    except KeyboardInterrupt:
        logger.info("\n=== STOPPED BY USER (Ctrl+C) ===")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
