from base64 import b64encode
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta
from db import DatabaseManager
import sys
import os
import json
import time
# from alert_rules import evaluate_all_alerts

# Logs configuration
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("logs/fitbit.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

def refresh_access_token(refresh_token):
    """
    Refresh the access token using the refresh token according to the OAuth 2.0 standard (RFC 6749).
    """
    url = "https://api.fitbit.com/oauth2/token"
    CLIENT_ID = os.getenv("CLIENT_ID")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")

    # Client authentication using Basic Auth
    auth_header = b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    # Request parameters
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }

    # Make the POST request
    response = requests.post(url, headers=headers, data=data)

    # Check the response
    if response.status_code == 200:
        # If the request is successful, return the new tokens.
        new_tokens = response.json()
        print(f"Token refreshed successfully: {new_tokens}")
        return new_tokens.get("access_token"), new_tokens.get("refresh_token")
    else:
        # If the request fails, print the error and return None.
        print(f"Error refreshing token: {response.status_code}, {response.text}")
        return None, None



def get_fitbit_data(access_token, email):
    headers = {"Authorization": f"Bearer {access_token}"}
    def fetch_and_store(date_str):
        # db = DatabaseManager()
        # if not db.connect():
        #     logger.error("Failed to connect to database")
        #     return False
        
        # device_id = db.get_email_id_by_name(email)
        # if not device_id:
        #     logger.error(f"Error: No device_id found for the email {email}")
        #     return False
        # data = {
        #     'steps': 0,
        #     'distance': 0,
        #     'calories': 0,
        #     'floors': 0,
        #     'elevation': 0,
        #     'active_minutes': 0,
        #     'sedentary_minutes': 0,
        #     'heart_rate': 0,
        #     'sleep_minutes': 0,
        #     'nutrition_calories': 0,
        #     'water': 0,
        #     'spo2': 0,
        #     'respiratory_rate': 0,
        #     'temperature': 0
        # }
        try:

            # Daily activity data
            activity_url = f"https://api.fitbit.com/1/user/-/activities/date/{date_str}.json"
            response = requests.get(activity_url, headers=headers)
            response.raise_for_status()
            activity_data = response.json()
            if 'summary' in activity_data:
                summary = activity_data['summary']
                data.update({
                    'steps': summary.get('steps', 0),
                    'distance': summary.get('distances', [{}])[0].get('distance', 0),
                    'calories': summary.get('caloriesOut', 0),
                    'floors': summary.get('floors', 0),
                    'elevation': summary.get('elevation', 0),
                    'active_minutes': summary.get('veryActiveMinutes', 0),
                    'sedentary_minutes': summary.get('sedentaryMinutes', 0)
                })

            # Heart rate
            heart_rate_url = f"https://api.fitbit.com/1/user/-/activities/heart/date/{date_str}/1d.json"
            response = requests.get(heart_rate_url, headers=headers)
            response.raise_for_status()
            heart_rate_data = response.json()
            if 'activities-heart' in heart_rate_data and heart_rate_data['activities-heart']:
                data['heart_rate'] = heart_rate_data['activities-heart'][0].get('value', {}).get('restingHeartRate', 0)

            # Sleep
            sleep_url = f"https://api.fitbit.com/1.2/user/-/sleep/date/{date_str}.json"
            response = requests.get(sleep_url, headers=headers)
            response.raise_for_status()
            sleep_data = response.json()
            if 'sleep' in sleep_data:
                data['sleep_minutes'] = sum(log.get('minutesAsleep', 0) for log in sleep_data['sleep'])

            # Nutrition
            nutrition_url = f"https://api.fitbit.com/1/user/-/foods/log/date/{date_str}.json"
            response = requests.get(nutrition_url, headers=headers)
            response.raise_for_status()
            nutrition_data = response.json()

            if 'summary' in nutrition_data:
                data['nutrition_calories'] = nutrition_data['summary'].get('calories', 0)

            water_url = f"https://api.fitbit.com/1/user/-/foods/log/water/date/{date_str}.json"
            response = requests.get(water_url, headers=headers)
            response.raise_for_status()
            water_data = response.json()
            if 'summary' in water_data:
                data['water'] = water_data['summary'].get('water', 0)

            # SpO2
            spo2_url = f"https://api.fitbit.com/1/user/-/spo2/date/{date_str}.json"
            response = requests.get(spo2_url, headers=headers)
            if response.status_code == 200:
                spo2_data = response.json()
                if isinstance(spo2_data.get('value'), dict):
                    data['spo2'] = float(spo2_data['value'].get('avg', 0))
                else:
                    data['spo2'] = float(spo2_data.get('value', 0))

            # Respiratory rate
            respiratory_rate_url = f"https://api.fitbit.com/1/user/-/br/date/{date_str}.json"
            response = requests.get(respiratory_rate_url, headers=headers)
            if response.status_code == 200:
                respiratory_data = response.json()
                if isinstance(respiratory_data.get('value'), dict):
                    data['respiratory_rate'] = float(respiratory_data['value'].get('breathingRate', 0))
                else:
                    data['respiratory_rate'] = float(respiratory_data.get('value', 0))

            # Temperature
            temperature_url = f"https://api.fitbit.com/1/user/-/temp/core/date/{date_str}.json"
            response = requests.get(temperature_url, headers=headers)
            if response.status_code == 200:
                temperature_data = response.json()
                data['temperature'] = temperature_data.get('value', 0)

            # Save to the database
            db.insert_daily_summary(
                email_id=email_id,
                date=date_str,
                **data
            )

            # Evaluar alertas después de guardar los datos
            current_date = datetime.strptime(date_str, "%Y-%m-%d")
            """alerts = evaluate_all_alerts(user_id, current_date)
            if alerts:
                logger.info(f"Alertas generadas para {email}: {alerts}")

            # Verificar calidad de datos
            if any(v == 0 for v in [data['steps'], data['active_minutes'], data['heart_rate']]):
                if db.connect():
                    try:
                        db.insert_alert(
                            user_id=user_id,
                            alert_type='data_quality',
                            priority='high',
                            triggering_value=0,
                            threshold='30',
                            timestamp=current_date,
                            details="alerts.data_quality.zero_values"
                        )
                    finally:
                        db.close()
            logger.info(f"Data collected for {email} in {date_str}:")
            for key, value in data.items():
                logger.info(f"{key}: {value}")
            return True"""
        
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                raise
            elif e.response.status_code == 429:
                logger.warning(f"Rate limit (429) reached for {email} on {date_str}.")
                print(response.headers)

                raise
            logger.error(f"HTTP error while fetching data from Fitbit: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error while fetching data from Fitbit. {e}")
            return False
    return fetch_and_store



def process_emails():

    db = DatabaseManager()
    if db.connect():
        
        email_addresses = db.get_all_emails()

        if len(email_addresses) > 0:
            
            # Date range
            START_DATE = datetime(2025, 8, 20)
            END_DATE = datetime.now()
            # END_DATE = datetime(2025, 3, 31)

            for email_address in email_addresses:
                logger.info(f"\n=== Processing email address: {email_address} ===")

                access_token, refresh_token = db.get_email_tokens(email_address['id'])
                
                if not access_token or not refresh_token:
                    logger.warning(f"No valid tokens were found for the email: {email_address['address_name']}.")
                

                # Checkpoint path
                checkpoint_path = f"logs/checkpoint_{email_address['address_name'].replace('@','_at_')}.json"
                # Leer checkpoint
                if os.path.exists(checkpoint_path):
                    with open(checkpoint_path, 'r') as f:
                        checkpoint = json.load(f)
                    last_date_str = checkpoint.get('last_date')
                    if last_date_str:
                        current_date = datetime.strptime(last_date_str, "%Y-%m-%d")
                    else:
                        current_date = START_DATE
                else:
                    current_date = START_DATE

                # current_date = datetime(2025, 6, 1)

                fetch_and_store = get_fitbit_data(access_token, email_address['address_name'])

                rate_limit_hit = False
                current_access_token = access_token
                current_refresh_token = refresh_token

                while current_date <= END_DATE:
                    print("START DATE: ", current_date)
                    date_str = current_date.strftime("%Y-%m-%d")
                    logger.info(f"Processing {date_str} for {email_address['address_name']}")
                    try:
                        
                        success = fetch_and_store(date_str)
                        if success:
                            logger.info(f"Data successfully collected for {email_address['address_name']} on {date_str}.")
                        # Guardar checkpoint
                        with open(checkpoint_path, 'w') as f:
                            json.dump({'last_date': date_str}, f)
                    except requests.exceptions.HTTPError as e:
                        if e.response.status_code == 401:
                            logger.warning(f"Token expired for email {email_address['address_name']}. Attempting to refresh the token...")
                            new_access_token, new_refresh_token = refresh_access_token(current_refresh_token)
                            
                        elif e.response.status_code == 429:
                            logger.warning(f"Rate limit reached for {email_address['address_name']} on {date_str}. Saving checkpoint and skipping to the next user.")
                            with open(checkpoint_path, 'w') as f:
                                json.dump({'last_date': date_str}, f)
                            rate_limit_hit = True
                            break
                        else:
                            logger.error(f"HTTP error while fetching data from Fitbit for email {email_address['address_name']}: {e}")
                    except Exception as e:
                        logger.error(f"Unexpected error while processing email {email_address['address_name']} on {date_str}: {e}")
                    # Sleep para evitar rate limit
                    time.sleep(1)
                    current_date += timedelta(days=1)

                if not rate_limit_hit and current_date > END_DATE:
                    logger.info(f"User {email_address['address_name']} is up to date. All data collected up to {END_DATE.strftime('%Y-%m-%d')}.")

            
            
        else:
            logger.error("No emails were found in the database.")
            sys.exit(1)

        db.close()

    else:
        logger.error("Failed to connect to database")
        sys.exit(1)

    

if __name__ == "__main__":
    # Create logs directory if it doesn't exist.
    os.makedirs("logs", exist_ok=True)
    # Get the list of unique emails.
    
    process_emails()