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
from db import get_unique_emails, get_device_id_by_email, insert_intraday_metric, get_device_tokens
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
def get_checkpoint(email):
    checkpoint_path = f"logs/checkpoint_intraday_{email}.json"
    if os.path.exists(checkpoint_path):
        try:
            with open(checkpoint_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not read checkpoint for {email}: {e}")
    return {}

def update_checkpoint(email, checkpoint):
    checkpoint_path = f"logs/checkpoint_intraday_{email}.json"
    try:
        with open(checkpoint_path, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"Could not save the checkpoint for {email}: {e}")

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
def get_intraday_data(access_token, email, date_str=None):
    headers = {"Authorization": f"Bearer {access_token}"}
    if date_str is None:
        today = datetime.now().strftime("%Y-%m-%d")
    else:
        today = date_str
    user_id = get_device_id_by_email(email)
    if not user_id:
        logger.error(f"Error: No device_id found for the email {email}")
        return False
    checkpoint = get_checkpoint(email)
    try:
        logger.info(f"\n=== INITIALIZING INTRADAY DATA COLLECTION FOR {email} ({today}) ===")
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
                            insert_intraday_metric(user_id, timestamp, data_type='heart_rate', value=value)
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
                            insert_intraday_metric(user_id, timestamp, data_type='steps', value=value)
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
                            insert_intraday_metric(user_id, timestamp, data_type='calories', value=value)
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
                            insert_intraday_metric(user_id, timestamp, data_type='distance', value=value)
                            total_distance_points += 1
                            checkpoint["distance"] = timestamp.strftime("%Y-%m-%d %H:%M:%S")

        # # 5. INTRADAY ACTIVE ZONE MINUTES (Active Zone Minutes)
        # azm_url = f"https://api.fitbit.com/1/user/-/activities/active-zone-minutes/date/{today}/1d/{detail_level}.json"
        # azm_response = requests.get(azm_url, headers=headers)
        # print(azm_response)
        # last_azm_ts = checkpoint.get("active_zone_minutes")
        # if azm_response.status_code == 200:
        #     azm_data = azm_response.json()
        #     intraday_key = 'activities-active-zone-minutes-intraday'
        #     if intraday_key in azm_data:
        #         intraday_data = azm_data[intraday_key]
        #         print(intraday_data)
        #         dataset = intraday_data.get('dataset', [])
        #         for point in dataset:
        #             time_str = point.get('time')
        #             value = point.get('value')
        #             if time_str and value is not None:
        #                 timestamp = datetime.strptime(f"{today} {time_str}", "%Y-%m-%d %H:%M:%S")
        #                 if not last_azm_ts or timestamp > datetime.strptime(last_azm_ts, "%Y-%m-%d %H:%M:%S"):
        #                     insert_intraday_metric(user_id, timestamp, data_type='active_zone_minutes', value=value)
        #                     total_active_zone_points += 1
        #                     checkpoint["active_zone_minutes"] = timestamp.strftime("%Y-%m-%d %H:%M:%S")

        update_checkpoint(email, checkpoint)
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
BACKFILL_END_DATE = "2025-07-31"    # Last day to collect (inclusive)


# --- MAIN WORKFLOW ---
def process_all_users():
    unique_emails = get_unique_emails()
    if not unique_emails:
        logger.error("No emails found in the database.")
        return

    today = datetime.now().date()
    for email in unique_emails:
        logger.info(f"\n=== Processing user: {email} ===")
        # access_token, refresh_token = get_user_tokens(email)
        access_token, refresh_token = get_device_tokens(email)
        if not access_token or not refresh_token:
            logger.warning(f"No valid tokens found for the email {email}. It is necessary to re-link the device.")
            continue
        current_access_token = access_token
        current_refresh_token = refresh_token
        # Read checkpoint
        checkpoint_path = f"logs/checkpoint_intraday_{email}.json"
        if os.path.exists(checkpoint_path):
            with open(checkpoint_path, 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)
            last_date_str = checkpoint_data.get('last_date')
            if last_date_str:
                last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
            else:
                last_date = None
        else:
            last_date = None

        # Determine the date range to process.

        if BACKFILL_START_DATE and BACKFILL_END_DATE:
            print("ENTERED" )
            # Backfill mode: only collect data between those dates.
            start_date = datetime.strptime(BACKFILL_START_DATE, "%Y-%m-%d").date()
            end_date = datetime.strptime(BACKFILL_END_DATE, "%Y-%m-%d").date()

            print("START DATE:", start_date, "END DATE:", end_date)
            if last_date is not None and last_date >= start_date:
                # If the checkpoint is already within the range, continue from the next day.
                current_date = last_date + timedelta(days=1)
            else:
                current_date = start_date
            # Only process up to end_date.

            print("START DATE:", start_date, "END DATE:", end_date, "CURRENT DATE:", current_date)
            while current_date <= end_date:
                print("CURRENT DATE:", current_date)
                date_str = current_date.strftime('%Y-%m-%d')
                try:
                    logger.info(f"Collecting intraday data for {email} on {date_str}")
                    success = get_intraday_data(current_access_token, email, date_str)
                    # Save checkpoint
                    with open(checkpoint_path, 'w', encoding='utf-8') as f:
                        json.dump({'last_date': date_str}, f)
                    if not success:
                        logger.warning(f"Could not collect data for {email} on {date_str}")
                except requests.exceptions.HTTPError as e:
                    if hasattr(e, 'response') and e.response and e.response.status_code == 401:
                        logger.warning(f"Token expired for {email}. Attempting to refresh the token...")
                        new_access_token, new_refresh_token = refresh_access_token(current_refresh_token)
                        if new_access_token and new_refresh_token:
                            update_users_tokens(email, new_access_token, new_refresh_token)
                            current_access_token = new_access_token
                            current_refresh_token = new_refresh_token
                            try:
                                success = get_intraday_data(current_access_token, email, date_str)
                                with open(checkpoint_path, 'w', encoding='utf-8') as f:
                                    json.dump({'last_date': date_str}, f)
                                if not success:
                                    logger.warning(f"Could not collect data after refreshing token for {email} on {date_str}.")
                            except Exception as e2:
                                logger.error(f"Error after refreshing token for {email}: {e2}")
                        else:
                            logger.error(f"Could not refresh token for {email}. Please reauthorize the device.")
                            break
                    elif hasattr(e, 'response') and e.response and e.response.status_code == 429:
                        logger.warning(f"Rate limit reached for {email} on {date_str}. Stopping processing.")
                        break
                    else:
                        logger.error(f"HTTP error while fetching intraday data for {email}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error while processing {email} on {date_str}: {e}", exc_info=True)
                time.sleep(1)
                current_date += timedelta(days=1)
            logger.info(f"User {email} processed up to {end_date} (backfill mode).")
        else:
            # Normal mode: collect only the current day if already up to date.
            if last_date is None or last_date < today:
                current_date = today
                date_str = current_date.strftime('%Y-%m-%d')
                try:
                    logger.info(f"Collecting intraday data for {email} on {date_str}")
                    success = get_intraday_data(current_access_token, email, date_str)
                    with open(checkpoint_path, 'w', encoding='utf-8') as f:
                        json.dump({'last_date': date_str}, f)
                    if not success:
                        logger.warning(f"Could not collect data for {email} on {date_str}")
                except requests.exceptions.HTTPError as e:
                    if hasattr(e, 'response') and e.response and e.response.status_code == 401:
                        logger.warning(f"Token expired for {email}. Attempting to refresh the token...")
                        new_access_token, new_refresh_token = refresh_access_token(current_refresh_token)
                        if new_access_token and new_refresh_token:
                            update_users_tokens(email, new_access_token, new_refresh_token)
                            current_access_token = new_access_token
                            current_refresh_token = new_refresh_token
                            try:
                                success = get_intraday_data(current_access_token, email, date_str)
                                with open(checkpoint_path, 'w', encoding='utf-8') as f:
                                    json.dump({'last_date': date_str}, f)
                                if not success:
                                    logger.warning(f"Could not collect data after refreshing token for {email} on {date_str}.")
                            except Exception as e2:
                                logger.error(f"Error after refreshing token for {email}: {e2}")
                        else:
                            logger.error(f"Could not refresh the token for {email}. Please reauthorize the device.")
                    elif hasattr(e, 'response') and e.response and e.response.status_code == 429:
                        logger.warning(f"Rate limit reached for {email} on {date_str}. Stopping processing.")
                    else:
                        logger.error(f"HTTP error while fetching intraday data for {email}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error while processing {email} on {date_str}: {e}", exc_info=True)
                time.sleep(1)
            logger.info(f"User {email} processed for the day {today} (normal mode).")
    logger.info("=== END OF FITBIT INTRADAY EXECUTION ===")


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    logger.info("=== START OF FITBIT INTRADAY EXECUTION (MULTI-USER DB MODE) ===")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Working directory: {os.getcwd()}")
    process_all_users()
