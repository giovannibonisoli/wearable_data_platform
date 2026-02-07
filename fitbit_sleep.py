from base64 import b64encode
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta

import sys
import os
import json
import time
import logging

from database import Database, ConnectionManager, DeviceRepository, SleepRepository
from auth import refresh_tokens, get_device_info


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("logs/fitbit_summary.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


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

    return None, True


def fetch_sleep_logs(access_token, device_id, date_obj, sleep_repo):

    end_date = date_obj + timedelta(days=1)
    url = f"https://api.fitbit.com/1.2/user/-/sleep/date/{date_obj}.json"
    
    data, hit_rate_limit = request_with_rate_limit(url, access_token)

    if hit_rate_limit:
        return False, True
    else:
        for sleep_log in data['sleep']:
            sleep_session_id = sleep_repo.create_session(device_id)
            
            if sleep_session_id:
                sleep_repo.insert_sleep_log(sleep_session_id, sleep_log)

                for level in sleep_log['levels']['data']:
                    sleep_repo.insert_sleep_level(sleep_session_id, level)

                if sleep_log['type'] == 'stages':
                    for short in sleep_log['levels']['shortData']:
                        sleep_repo.insert_sleep_short_level(sleep_session_id, short)

        if len(data['sleep']) == 0:
            logger.info(f"No sleep logs found for device {device_id} in date {date_obj}")

        return True, False


def get_sleep_data(device, device_repo, sleep_repo):

    logger.info(f"Processing sleep logs for device {device.id} with email address {device.email_address}")
    
    access_token, refresh_token = device_repo.get_tokens(device.id)
    if not access_token or not refresh_token:
        logger.warning(f"No tokens for {device.email_address}")
        return 'error'

    last_date = device.sleep_checkpoint

    if last_date:
        start_date = last_date + timedelta(days=1)
    else:
        start_date = datetime(2025,1,24).date()

    end_date = (device.last_synch.date() - timedelta(days=1))

    if start_date >= end_date:
        logger.info(f"{device.email_address} is up to date for summaries")
        return 'success'

    current_date = start_date
    
    while current_date <= end_date:
        try:
            success, rate_limited = fetch_sleep_logs(access_token, device.id, current_date, sleep_repo)
            
            if rate_limited:
                logger.info(f"Rate limit reached for {device.id} with email address {device.email_address} on {current_date}. Skipping to next email.")
                return 'rate_limited'
            
            if not success:
                # Other error, skip this date and continue
                logger.warning(f"Failed to fetch sleep logs for {device.id} with email address {device.email_address} on {current_date}, continuing...")
                current_date += timedelta(days=1)
                continue

            device_repo.update_sleep_checkpoint(device.id, current_date)
                
            current_date += timedelta(days=1)
            time.sleep(1)  # minimal sleep to buffer
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.warning(f"Token expired for {device.id} with email address {device.email_address}, refreshing …")
                new_access, new_refresh = refresh_tokens(refresh_token)
                if new_access and new_refresh:
                    device_repo.update_tokens(device.id, new_access, new_refresh)
                    access_token = new_access
                    refresh_token = new_refresh
                    logger.info(f"Token refreshed for device {device.id} with email address {device.email_address}")
                    continue  # retry same date
                else:
                    logger.error(f"Failed token refresh for device {device.id} with email address {device.email_address}")
                    return 'error'
            else:
                logger.error(f"HTTP error for device {device.id} with email address {device.email_address} on {current_date}: {e}")
                return 'error'
        except Exception as e:
            logger.error(f"Unexpected error for device {device.id} with email address {device.email_address} on {current_date}: {e}")
            return 'error'
            
    logger.info(f"Completed summaries for device {device.id} with email address {device.email_address} up to {end_date}")
    return 'success'



def process_all_devices():

    while True:

        try:
            with ConnectionManager() as conn:
                device_repo = DeviceRepository(conn)
                sleep_repo = SleepRepository(conn)

                devices = device_repo.get_all_authorized()
                if not devices:
                    logger.warning("No devices found")
                    time.sleep(60)
                    continue

                results = {
                    'success': 0,
                    'rate_limited': 0,
                    'error': 0
                }
                
                for device in devices:
                    result = get_sleep_data(device, device_repo, sleep_repo)
                    results[result] += 1

                # Log summary
                logger.info(f"Cycle complete: {results['success']} successful, "
                        f"{results['rate_limited']} rate-limited, {results['error']} errors")

                # Only sleep if ALL devices are rate-limited
                if results['rate_limited'] == len(devices) and results['rate_limited'] > 0:
                    sleep_interval = 10
                    logger.info(f"ALL devices are rate-limited. Sleeping {sleep_interval} minutes before retry.")
                    time.sleep(sleep_interval*60)
                else:
                    # At least one email processed successfully or had errors
                    logger.info("At least one email processed. Sleeping 30 minutes before next cycle.")
                    time.sleep(1800)
                
        except KeyboardInterrupt:
            logger.info("=== STOPPED BY USER ===")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
            time.sleep(60)


if __name__ == "__main__":
    logger.info("=== START OF FITBIT SLEEP DATA CONTINUOUS LOOP ===")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Working directory: {os.getcwd()}")
    
    try:
        process_all_devices()
    except KeyboardInterrupt:
        logger.info("\n=== STOPPED BY USER (Ctrl+C) ===")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
