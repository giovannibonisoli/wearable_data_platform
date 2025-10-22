"""
FITBIT INTRADAY - MODULAR BACKFILL & CHECKPOINT

This script collects intraday Fitbit data for multiple users, with robust checkpoint and backfill logic.

FUNCIONALIDAD PRINCIPAL:
- For each user, it uses a checkpoint to know up to which date data has been collected.
- If you set BACKFILL_START_DATE and BACKFILL_END_DATE, it only collects data between those dates (both inclusive), useful for historical backfill.
- If both variables are set to None, the script works in normal mode: it only collects data for the current day if the user is already up to date
(ideal for periodic execution like cron jobs).
- The checkpoint is saved per user and allows resuming if execution is interrupted.
- The backfill range can be easily modified by editing the variables at the beginning of the script.
- When you finish backfilling, set both variables to None to return to normal mode.

Key variables to modify:
    BACKFILL_START_DATE = "2025-05-21"  # First day to collect (inclusive)
    BACKFILL_END_DATE = "2025-05-28"    # Last day to collect (inclusive)

If you have questions, review this block or search for 'BACKFILL' in the code.
"""

from base64 import b64encode
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta
from db import DatabaseManager
import sys
import os
import json
import time
import logging

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

# --- CHECKPOINT HELPERS ---
def get_checkpoint(address_name):
    checkpoint_path = f"logs/checkpoint_intraday_{address_name}.json"
    if os.path.exists(checkpoint_path):
        try:
            with open(checkpoint_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not read checkpoint for {address_name}: {e}")
    return {}

def update_checkpoint(address_name, checkpoint):
    checkpoint_path = f"logs/checkpoint_intraday_{address_name}.json"
    try:
        with open(checkpoint_path, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"Could not save the checkpoint for {address_name}: {e}")

# --- TOKEN REFRESH ---
def refresh_access_token(refresh_token):
    url = "https://api.fitbit.com/oauth2/token"
    auth_header = b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }
    logger.info(f"Refreshing token for refresh_token: {refresh_token[:10]}...")
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        new_tokens = response.json()
        logger.info(f"Token refreshed successfully")
        return new_tokens.get("access_token"), new_tokens.get("refresh_token")
    else:
        logger.error(f"Error refreshing token: {response.status_code}, {response.text}")
        return None, None

def request_with_rate_limit(url, access_token, max_retries=5):
    headers = {"Authorization": f"Bearer {access_token}"}
    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, headers=headers)
        limit = resp.headers.get('Fitbit-Rate-Limit-Limit')
        remaining = resp.headers.get('Fitbit-Rate-Limit-Remaining')
        reset = resp.headers.get('Fitbit-Rate-Limit-Reset')
        print(f"Rate‑limit: {remaining}/{limit}, resets in {reset}s")

        if resp.status_code == 429:
            wait = int(reset) if reset else (60 * (1.5 ** (attempt-1)))
            print(f"429 hit, waiting {wait:.1f}s before retrying (attempt {attempt})")
            time.sleep(wait)
            continue

        resp.raise_for_status()
        return resp.json()

    raise RuntimeError(f"Exceeded {max_retries} retries due to rate limits")

# --- INTRADAY DATA COLLECTION ---
def get_intraday_data(access_token, email_address, date_str=None):
    headers = {"Authorization": f"Bearer {access_token}"}
    if date_str is None:
        today = datetime.now().strftime("%Y-%m-%d")
    else:
        today = date_str
    db = DatabaseManager()
    if not db.connect():
        logger.error("Failed to connect to database")
        return False
    
    
    checkpoint = get_checkpoint(email_address['address_name'])
    try:
        logger.info(f"\n=== INITIALIZING INTRADAY DATA COLLECTION FOR {email_address['address_name']} ({today}) ===")
        total_heart_rate_points = 0
        total_steps_points = 0
        total_calories_points = 0
        total_active_zone_points = 0
        total_distance_points = 0
        detail_level = "1min"

        # 1. INTRADAY HEART RATE (Heart Rate)
        heart_rate_url = f"https://api.fitbit.com/1/user/-/activities/heart/date/{today}/1d/{detail_level}.json"
        request_with_rate_limit(heart_rate_url, access_token)
        heart_response = requests.get(heart_rate_url, headers=headers)
        last_hr_ts = checkpoint.get("heart_rate")

        if heart_response.status_code == 200:
            heart_data = heart_response.json()

            if 'activities-heart-intraday' in heart_data:
                intraday_data = heart_data['activities-heart-intraday']
                dataset = intraday_data.get('dataset', [])
                for point in dataset:
                    time_str = point.get('time')
                    value = point.get('value')
                    if time_str and value is not None:
                        timestamp = datetime.strptime(f"{today} {time_str}", "%Y-%m-%d %H:%M:%S")
                        if not last_hr_ts or timestamp > datetime.strptime(last_hr_ts, "%Y-%m-%d %H:%M:%S"):
                            # insert_intraday_metric(user_id, timestamp, 'heart_rate', value)
                            db = DatabaseManager()
                            if db.connect():
                                try:
                                    db.insert_intraday_metric(email_address['id'], timestamp, data_type='heart_rate', value=value)
                                finally:
                                    db.close()
                            total_heart_rate_points += 1
                            checkpoint["heart_rate"] = timestamp.strftime("%Y-%m-%d %H:%M:%S")

        # 2. INTRADAY STEPS (Steps)
        steps_url = f"https://api.fitbit.com/1/user/-/activities/steps/date/{today}/1d/{detail_level}.json"
        steps_response = requests.get(steps_url, headers=headers)
        last_steps_ts = checkpoint.get("steps")
        if steps_response.status_code == 200:
            steps_data = steps_response.json()
            if 'activities-steps-intraday' in steps_data:
                intraday_data = steps_data['activities-steps-intraday']
                dataset = intraday_data.get('dataset', [])
                for point in dataset:
                    time_str = point.get('time')
                    value = point.get('value')
                    if time_str and value is not None:
                        timestamp = datetime.strptime(f"{today} {time_str}", "%Y-%m-%d %H:%M:%S")
                        if not last_steps_ts or timestamp > datetime.strptime(last_steps_ts, "%Y-%m-%d %H:%M:%S"):
                            # insert_intraday_metric(user_id, timestamp, 'steps', value)
                            db = DatabaseManager()
                            if db.connect():
                                try:
                                    db.insert_intraday_metric(email_address['id'], timestamp, data_type='steps', value=value)
                                finally:
                                    db.close()
                            total_steps_points += 1
                            checkpoint["steps"] = timestamp.strftime("%Y-%m-%d %H:%M:%S")

        # 3. INTRADAY CALORIES (Calories)
        calories_url = f"https://api.fitbit.com/1/user/-/activities/calories/date/{today}/1d/{detail_level}.json"
        calories_response = requests.get(calories_url, headers=headers)
        last_calories_ts = checkpoint.get("calories")
        if calories_response.status_code == 200:
            calories_data = calories_response.json()
            if 'activities-calories-intraday' in calories_data:
                intraday_data = calories_data['activities-calories-intraday']
                dataset = intraday_data.get('dataset', [])
                for point in dataset:
                    time_str = point.get('time')
                    value = point.get('value')
                    if time_str and value is not None:
                        timestamp = datetime.strptime(f"{today} {time_str}", "%Y-%m-%d %H:%M:%S")
                        if not last_calories_ts or timestamp > datetime.strptime(last_calories_ts, "%Y-%m-%d %H:%M:%S"):
                            db = DatabaseManager()
                            if db.connect():
                                try:
                                    db.insert_intraday_metric(email_address['id'], timestamp, data_type='calories', value=value)
                                finally:
                                    db.close()
                            total_calories_points += 1
                            checkpoint["calories"] = timestamp.strftime("%Y-%m-%d %H:%M:%S")

        # 4. INTRADAY DISTANCE (Distance)
        distance_url = f"https://api.fitbit.com/1/user/-/activities/distance/date/{today}/1d/{detail_level}.json"
        distance_response = requests.get(distance_url, headers=headers)
        last_distance_ts = checkpoint.get("distance")
        if distance_response.status_code == 200:
            distance_data = distance_response.json()
            if 'activities-distance-intraday' in distance_data:
                intraday_data = distance_data['activities-distance-intraday']
                dataset = intraday_data.get('dataset', [])
                for point in dataset:
                    time_str = point.get('time')
                    value = point.get('value')
                    if time_str and value is not None:
                        timestamp = datetime.strptime(f"{today} {time_str}", "%Y-%m-%d %H:%M:%S")
                        if not last_distance_ts or timestamp > datetime.strptime(last_distance_ts, "%Y-%m-%d %H:%M:%S"):
                            db = DatabaseManager()
                            if db.connect():
                                try:
                                    db.insert_intraday_metric(email_address['id'], timestamp, data_type='distance', value=value)
                                finally:
                                    db.close()
                            total_distance_points += 1
                            checkpoint["distance"] = timestamp.strftime("%Y-%m-%d %H:%M:%S")

        update_checkpoint(email_address['address_name'], checkpoint)
        total_points = (total_heart_rate_points + total_steps_points + total_calories_points + total_distance_points + total_active_zone_points)
        logger.info(f"Total points collected: {total_points}")
        if total_points > 0:
            # logger.info("\n✅ INTRADAY DATA COLLECTION SUCCESSFUL")
            logger.info("\n INTRADAY DATA COLLECTION SUCCESSFUL")
            return True
        else:
            # logger.warning("\n❌ COULD NOT COLLECT INTRADAY DATA")
            logger.warning("\n COULD NOT COLLECT INTRADAY DATA")
            return False
    except requests.exceptions.HTTPError as e:
        if hasattr(e, 'response') and e.response and e.response.status_code == 401:
            logger.error(f"Authentication error (401): {str(e)}")
            raise
        logger.error(f"HTTP error while fetching intraday data: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error while fetching intraday data: {str(e)}", exc_info=True)
        return False

# === BACKFILL CONFIGURATION ===
# If you want to backfill a specific range, set the dates in 'YYYY-MM-DD' format.
# If you don't want to backfill, leave both as None.

BACKFILL_START_DATE = "2025-06-19"  # First day to collect (inclusive)
BACKFILL_END_DATE = "2025-10-22"    # Last day to collect (inclusive)


# --- MAIN WORKFLOW ---
def process_all_users():
    db = DatabaseManager()
    if db.connect():
        
        email_addresses = db.get_all_emails()

        if len(email_addresses) > 0:

            today = datetime.now().date()
            for email_address in email_addresses:
                logger.info(f"\n=== Processing user: {email_address['address_name']} ===")
                # access_token, refresh_token = get_user_tokens(email)

                access_token, refresh_token = db.get_email_tokens(email_address['id'])
                
                if not access_token or not refresh_token:
                    logger.warning(f"No valid tokens were found for the email: {email_address['address_name']}.")
                
                current_access_token = access_token
                current_refresh_token = refresh_token
                

                # Checkpoint path
                checkpoint_path = f"logs/checkpoint_{email_address['address_name'].replace('@','_at_')}.json"
                # Leer checkpoint
                if os.path.exists(checkpoint_path):
                    with open(checkpoint_path, 'r') as f:
                        checkpoint = json.load(f)
                    last_date_str = checkpoint.get('last_date')
                    if last_date_str:
                        last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
                    else:
                        last_date = None
                else:
                    last_date = None
                    

                # Determine the date range to process.
                if BACKFILL_START_DATE and BACKFILL_END_DATE:
                    
                    start_date = datetime.strptime(BACKFILL_START_DATE, "%Y-%m-%d").date()
                    end_date = datetime.strptime(BACKFILL_END_DATE, "%Y-%m-%d").date()

                    print("START DATE:", start_date, "END DATE:", end_date)
                    if last_date is not None and last_date >= start_date:
                        # If the checkpoint is already within the range, continue from the next day.
                        current_date = last_date + timedelta(days=1)
                    else:
                        current_date = start_date
                    # Only process up to end_date.

                    while current_date <= end_date:
                        print("CURRENT DATE:", current_date)
                        date_str = current_date.strftime('%Y-%m-%d')
                        try:
                            logger.info(f"Collecting intraday data for {email_address['address_name']} on {date_str}")
                            success = get_intraday_data(current_access_token, email_address, date_str)
                            # Save checkpoint
                            with open(checkpoint_path, 'w', encoding='utf-8') as f:
                                json.dump({'last_date': date_str}, f)
                            if not success:
                                logger.warning(f"Could not collect data for {email_address['address_name']} on {date_str}")
                        except requests.exceptions.HTTPError as e:
                            if hasattr(e, 'response') and e.response and e.response.status_code == 401:
                                logger.warning(f"Token expired for {email_address['address_name']}. Attempting to refresh the token...")
                                new_access_token, new_refresh_token = refresh_access_token(current_refresh_token)
                                if new_access_token and new_refresh_token:
                                    update_users_tokens(email_address['address_name'], new_access_token, new_refresh_token)
                                    current_access_token = new_access_token
                                    current_refresh_token = new_refresh_token
                                    try:
                                        success = get_intraday_data(current_access_token, email_address, date_str)
                                        with open(checkpoint_path, 'w', encoding='utf-8') as f:
                                            json.dump({'last_date': date_str}, f)
                                        if not success:
                                            logger.warning(f"Could not collect data after refreshing token for {email_address['address_name']} on {date_str}.")
                                    except Exception as e2:
                                        logger.error(f"Error after refreshing token for {email_address['address_name']}: {e2}")
                                else:
                                    logger.error(f"Could not refresh token for {email_address['address_name']}. Please reauthorize the device.")
                                    break
                            elif hasattr(e, 'response') and e.response and e.response.status_code == 429:
                                logger.warning(f"Rate limit reached for {email_address['address_name']} on {date_str}. Stopping processing.")
                                break
                            else:
                                logger.error(f"HTTP error while fetching intraday data for {email_address['address_name']}: {e}")
                        except Exception as e:
                            logger.error(f"Unexpected error while processing {email_address['address_name']} on {date_str}: {e}", exc_info=True)
                        time.sleep(1)
                        current_date += timedelta(days=1)
                    logger.info(f"User {email_address['address_name']} processed up to {end_date} (backfill mode).")
                else:
                    # Normal mode: collect only the current day if already up to date.
                    if last_date is None or last_date < today:
                        current_date = today
                        date_str = current_date.strftime('%Y-%m-%d')
                        try:
                            logger.info(f"Collecting intraday data for {email_address['address_name']} on {date_str}")
                            success = get_intraday_data(current_access_token, email_address, date_str)
                            with open(checkpoint_path, 'w', encoding='utf-8') as f:
                                json.dump({'last_date': date_str}, f)
                            if not success:
                                logger.warning(f"Could not collect data for {email_address['address_name']} on {date_str}")
                        except requests.exceptions.HTTPError as e:
                            if hasattr(e, 'response') and e.response and e.response.status_code == 401:
                                logger.warning(f"Token expired for {email_address['address_name']}. Attempting to refresh the token...")
                                new_access_token, new_refresh_token = refresh_access_token(current_refresh_token)
                                if new_access_token and new_refresh_token:
                                    update_users_tokens(email_address['address_name'], new_access_token, new_refresh_token)
                                    current_access_token = new_access_token
                                    current_refresh_token = new_refresh_token
                                    try:
                                        success = get_intraday_data(current_access_token, email_address, date_str)
                                        with open(checkpoint_path, 'w', encoding='utf-8') as f:
                                            json.dump({'last_date': date_str}, f)
                                        if not success:
                                            logger.warning(f"Could not collect data after refreshing token for {email_address['address_name']} on {date_str}.")
                                    except Exception as e2:
                                        logger.error(f"Error after refreshing token for {email_address['address_name']}: {e2}")
                                else:
                                    logger.error(f"Could not refresh the token for {email_address['address_name']}. Please reauthorize the device.")
                            elif hasattr(e, 'response') and e.response and e.response.status_code == 429:
                                logger.warning(f"Rate limit reached for {email_address['address_name']} on {date_str}. Stopping processing.")
                            else:
                                logger.error(f"HTTP error while fetching intraday data for {email_address['address_name']}: {e}")
                        except Exception as e:
                            logger.error(f"Unexpected error while processing {email_address['address_name']} on {date_str}: {e}", exc_info=True)
                        time.sleep(1)
                    logger.info(f"User {email_address['address_name']} processed for the day {today} (normal mode).")
            logger.info("=== END OF FITBIT INTRADAY EXECUTION ===")


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    logger.info("=== START OF FITBIT INTRADAY EXECUTION (MULTI-USER DB MODE) ===")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Working directory: {os.getcwd()}")
    process_all_users()
