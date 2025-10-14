import json
import os
import logging
from datetime import datetime, timedelta
import psycopg2
import requests
import time
import base64

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("logs/fitbit_personal.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    "host": "l45jsd1dwi.b24njbbdat.tsdb.cloud.timescale.com",
    "database": "tsdb",
    "user": "tsdbadmin",
    "password": "sec05ujryx4177rx",
    "port": "38012",
    "sslmode": "require"
}

# Fitbit API configuration for multiple accounts
ACCOUNTS_CONFIG = {
    "wearable2livelyageign@gmail.com": {
        "client_id": "23QK2Y",
        "client_secret": "15dff85f95fb2b521461fa4e0c9abf2b",
        "access_token": "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIyM1FLMlkiLCJzdWIiOiJDRzZaTDYiLCJpc3MiOiJGaXRiaXQiLCJ0eXAiOiJhY2Nlc3NfdG9rZW4iLCJzY29wZXMiOiJ3aHIgd3BybyB3bnV0IHdzbGUgd3RlbSB3d2VpIHdzb2Mgd2FjdCB3c2V0IHdyZXMgd294eSIsImV4cCI6MTc0OTk0NDQ1NSwiaWF0IjoxNzQ5OTE1NjU1fQ.kNvyp_pc7JSZVvoITTLfAc-Ml8mxIPgdZy3Gt4AxZG4",
        "refresh_token": "7043731c1f8436104fec26ea5148f1286b2ba733eff109f23ead4b934d67d751",
        "user_id_placeholder": "-" # Fitbit API uses '-' for current user, actual DB user_id will be used for storage
    },
    "wearable1livelyageign@gmail.com": {
        "client_id": "23QJN8",
        "client_secret": "7f9d7193f3fd0fe1b73455dc85db89ba",
        "access_token": "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIyM1FKTjgiLCJzdWIiOiJDRzhUNkoiLCJpc3MiOiJGaXRiaXQiLCJ0eXAiOiJhY2Nlc3NfdG9rZW4iLCJzY29wZXMiOiJ3aHIgd3BybyB3bnV0IHdzbGUgd3RlbSB3d2VpIHdzb2Mgd2FjdCB3c2V0IHdveHkgd3JlcyIsImV4cCI6MTc0OTk0NDYxMSwiaWF0IjoxNzQ5OTE1ODExfQ.BtEpl3rv5FI6wb1IDY0LCkUSIc21Bw16milRCoSHxxA",
        "refresh_token": "92e26c5aa4f677b0df637ddc4156a985be168505b00693a8e36d9bbb9f11899d",
        "user_id_placeholder": "-"
    }
    # Add more accounts here if needed
}

# Global variable to store the currently selected user's email and their config
CURRENT_USER_EMAIL = None
CURRENT_USER_CONFIG = None

def load_tokens_from_db(email):
    """Load all credentials from the database for the specified email."""
    global CURRENT_USER_CONFIG
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT client_id, client_secret, access_token, refresh_token FROM users WHERE email = %s", (email,))
        creds = cur.fetchone()
        if creds and all(creds):
            CURRENT_USER_CONFIG = {
                "client_id": creds[0],
                "client_secret": creds[1],
                "access_token": creds[2],
                "refresh_token": creds[3],
                "user_id_placeholder": "-" # Keep placeholder, actual API calls use this
            }
            logger.info(f"Successfully loaded tokens and credentials from database for {email}.")
            return True
        else:
            logger.warning(f"No complete credentials found in database for {email}. Will use initial config if available.")
            # Fallback to initial config for this email if DB load fails
            if email in ACCOUNTS_CONFIG:
                CURRENT_USER_CONFIG = ACCOUNTS_CONFIG[email].copy()
                logger.info(f"Using initial hardcoded config for {email} as fallback.")
            else:
                logger.error(f"No configuration found for email {email} after DB load failure.")
                CURRENT_USER_CONFIG = None # Ensure it's None if no config available
            return False
    except Exception as e:
        logger.error(f"Error loading tokens from database for {email}: {e}.")
        if email in ACCOUNTS_CONFIG:
            CURRENT_USER_CONFIG = ACCOUNTS_CONFIG[email].copy() # Fallback
            logger.info(f"Using initial hardcoded config for {email} due to DB error.")
        else:
            logger.error(f"No configuration found for email {email} after DB error.")
            CURRENT_USER_CONFIG = None
        return False
    finally:
        if conn:
            cur.close()
            conn.close()

def refresh_fitbit_token():
    """Refresh the Fitbit access token for the CURRENT_USER_EMAIL."""
    global CURRENT_USER_CONFIG
    if not CURRENT_USER_CONFIG or not CURRENT_USER_EMAIL:
        logger.error("Cannot refresh token: No current user selected or config loaded.")
        return None

    logger.info(f"Attempting to refresh Fitbit token for {CURRENT_USER_EMAIL}.")
    auth_str = f"{CURRENT_USER_CONFIG['client_id']}:{CURRENT_USER_CONFIG['client_secret']}"
    auth_header = base64.b64encode(auth_str.encode()).decode()
    
    headers = {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "refresh_token",
        "refresh_token": CURRENT_USER_CONFIG['refresh_token']
    }
    
    try:
        response = requests.post("https://api.fitbit.com/oauth2/token", headers=headers, data=data)
        response.raise_for_status()
        
        new_tokens = response.json()
        CURRENT_USER_CONFIG['access_token'] = new_tokens['access_token']
        if 'refresh_token' in new_tokens:
            CURRENT_USER_CONFIG['refresh_token'] = new_tokens['refresh_token']
            logger.info(f"Fitbit token refreshed successfully for {CURRENT_USER_EMAIL}. New refresh token received.")
        else:
            logger.info(f"Fitbit token refreshed successfully for {CURRENT_USER_EMAIL}. Existing refresh token remains valid.")
        
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE users SET access_token = %s, refresh_token = %s
            WHERE email = %s
        """, (CURRENT_USER_CONFIG['access_token'], CURRENT_USER_CONFIG['refresh_token'], CURRENT_USER_EMAIL))
        conn.commit()
        logger.info(f"Updated tokens in the database for {CURRENT_USER_EMAIL}.")
        cur.close()
        conn.close()
        
        return CURRENT_USER_CONFIG['access_token']
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error refreshing Fitbit token for {CURRENT_USER_EMAIL}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response text: {e.response.text}")
        return None

def get_db_connection():
    """Create and return a database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def init_db():
    """Initialize the database with necessary tables."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Crear tabla de usuarios
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255) NOT NULL UNIQUE,
                client_id VARCHAR(255),
                client_secret VARCHAR(255),
                access_token TEXT,
                refresh_token TEXT,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Crear tabla de resúmenes diarios
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_summaries (
                id SERIAL,
                user_id INTEGER REFERENCES users(id),
                date DATE NOT NULL,
                steps INTEGER,
                heart_rate INTEGER,
                sleep_minutes INTEGER,
                calories INTEGER,
                distance FLOAT,
                floors INTEGER,
                elevation FLOAT,
                active_minutes INTEGER,
                sedentary_minutes INTEGER,
                nutrition_calories INTEGER,
                water FLOAT,
                weight FLOAT,
                bmi FLOAT,
                fat FLOAT,
                oxygen_saturation FLOAT,
                respiratory_rate FLOAT,
                temperature FLOAT,
                UNIQUE(user_id, date)
            );
        """)
        
        # Convertir a hipertabla
        cur.execute("""
            SELECT create_hypertable('daily_summaries', 'date', 
                if_not_exists => TRUE,
                migrate_data => TRUE
            );
        """)
        
        # Crear tabla de métricas intradía
        cur.execute("""
            CREATE TABLE IF NOT EXISTS intraday_metrics (
                id SERIAL,
                user_id INTEGER REFERENCES users(id),
                time TIMESTAMPTZ NOT NULL,
                type VARCHAR(50) NOT NULL,
                value FLOAT NOT NULL
            );
        """)
        
        # Convertir a hipertabla
        cur.execute("""
            SELECT create_hypertable('intraday_metrics', 'time',
                if_not_exists => TRUE,
                migrate_data => TRUE
            );
        """)

        # Crear tabla de registros de sueño
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sleep_logs (
                id SERIAL,
                user_id INTEGER REFERENCES users(id),
                start_time TIMESTAMPTZ NOT NULL,
                end_time TIMESTAMPTZ NOT NULL,
                duration_ms INTEGER,
                efficiency INTEGER,
                minutes_asleep INTEGER,
                minutes_awake INTEGER,
                minutes_in_rem INTEGER,
                minutes_in_light INTEGER,
                minutes_in_deep INTEGER
            );
        """)
        
        # Convertir a hipertabla
        cur.execute("""
            SELECT create_hypertable('sleep_logs', 'start_time',
                if_not_exists => TRUE,
                migrate_data => TRUE
            );
        """)

        # Crear tabla de alertas
        cur.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id SERIAL,
                alert_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                user_id INTEGER REFERENCES users(id),
                alert_type VARCHAR(100) NOT NULL,
                priority VARCHAR(20) NOT NULL,
                triggering_value DOUBLE PRECISION,
                threshold_value VARCHAR(50),
                details TEXT,
                acknowledged BOOLEAN DEFAULT FALSE,
                acknowledged_at TIMESTAMPTZ,
                acknowledged_by INTEGER REFERENCES users(id),
                PRIMARY KEY (id, alert_time)
            );
        """)
        
        # Convertir a hipertabla
        cur.execute("""
            SELECT create_hypertable('alerts', 'alert_time',
                if_not_exists => TRUE,
                migrate_data => TRUE
            );
        """)
        
        conn.commit()
        logger.info("Database initialized successfully with TimeScaleDB")
        return True
                
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        return False
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def get_fitbit_data(access_token, date_str):
    """Get Fitbit data for a specific date."""
    headers = {"Authorization": f"Bearer {access_token}"}
    
    data = {
        'steps': 0,
        'distance': 0,
        'calories': 0,
        'floors': 0,
        'elevation': 0,
        'active_minutes': 0,
        'sedentary_minutes': 0,
        'heart_rate': 0,
        'sleep_minutes': 0,
        'nutrition_calories': 0,
        'water': 0,
        'spo2': 0,
        'respiratory_rate': 0,
        'temperature': 0
    }

    try:
        # Datos de actividad diaria
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

        # Frecuencia cardíaca
        heart_rate_url = f"https://api.fitbit.com/1/user/-/activities/heart/date/{date_str}/1d.json"
        response = requests.get(heart_rate_url, headers=headers)
        response.raise_for_status()
        
        heart_rate_data = response.json()
        if 'activities-heart' in heart_rate_data and heart_rate_data['activities-heart']:
            data['heart_rate'] = heart_rate_data['activities-heart'][0].get('value', {}).get('restingHeartRate', 0)

        # Sueño
        sleep_url = f"https://api.fitbit.com/1.2/user/-/sleep/date/{date_str}.json"
        response = requests.get(sleep_url, headers=headers)
        response.raise_for_status()
        
        sleep_data = response.json()
        if 'sleep' in sleep_data:
            data['sleep_minutes'] = sum(log.get('minutesAsleep', 0) for log in sleep_data['sleep'])

        # Log de datos recopilados
        logger.info(f"\nDatos recopilados para {date_str}:")
        for key, value in data.items():
            logger.info(f"{key}: {value}")

        return data

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            logger.warning("Token expired or invalid in get_fitbit_data. Attempting refresh...")
            new_access_token = refresh_fitbit_token()
            if new_access_token:
                logger.info("Token refreshed. Retrying get_fitbit_data request...")
                return get_fitbit_data(new_access_token, date_str) # Retry with new token
            else:
                logger.error("Failed to refresh token in get_fitbit_data. Cannot proceed.")
                return None
        logger.error(f"Error HTTP al obtener datos de Fitbit: {e}")
        return None
    except Exception as e:
        logger.error(f"Error inesperado al obtener datos de Fitbit: {e}")
        return None

def get_intraday_data(access_token, date_str):
    """Get intraday data for a specific date."""
    headers = {"Authorization": f"Bearer {access_token}"}
    detail_level = "15min"  # Using 1min as per API documentation
    
    try:
        # Verificar scopes del token
        oauth_url = "https://api.fitbit.com/1.1/oauth2/introspect"
        oauth_headers = headers.copy()
        oauth_headers["Content-Type"] = "application/x-www-form-urlencoded"
        oauth_data = {"token": access_token}
        
        oauth_response = requests.post(oauth_url, headers=oauth_headers, data=oauth_data)
        logger.info(f"Token introspection status: {oauth_response.status_code}")
        
        if oauth_response.status_code == 200:
            oauth_data = oauth_response.json()
            scopes = oauth_data.get("scope", "")
            logger.info(f"Available scopes: {scopes}")
            if "HEARTRATE" not in scopes:
                logger.warning("Token does not have HEARTRATE scope. Intraday heart rate data will not be available.")
        else:
            logger.warning(f"Could not verify token scopes: {oauth_response.status_code}")

        # 1. FRECUENCIA CARDÍACA INTRADÍA
        # Reverting to the simpler intraday endpoint (no time segmentation) as per documentation
        heart_rate_url = f"https://api.fitbit.com/1/user/-/activities/heart/date/{date_str}/1d/{detail_level}.json"
        logger.info(f"Requesting heart rate intraday data from: {heart_rate_url}")
        
        response = requests.get(heart_rate_url, headers=headers)
        response.raise_for_status()
        
        # Print detailed response data
        logger.info("=== Heart Rate API Response Details ===")
        logger.info(f"URL: {heart_rate_url}")
        logger.info(f"Status Code: {response.status_code}")
        logger.info(f"Headers: {dict(response.headers)}")
        logger.info("Response Body:")
        logger.info(json.dumps(response.json(), indent=2))
        logger.info("=====================================")
        
        heart_data = response.json()
        
        # Check for intraday data in the response
        if 'activities-heart-intraday' in heart_data:
            intraday_data = heart_data['activities-heart-intraday']
            dataset = intraday_data.get('dataset', [])
            dataset_interval = intraday_data.get('datasetInterval', 'N/A')
            dataset_type = intraday_data.get('datasetType', 'N/A')
            
            logger.info(f"Intraday data details:")
            logger.info(f"- Dataset interval: {dataset_interval}")
            logger.info(f"- Dataset type: {dataset_type}")
            logger.info(f"- Number of data points: {len(dataset)}")
            
            if dataset:
                # Log first few data points for verification
                logger.info("Sample data points:")
                for point in dataset[:3]:
                    logger.info(f"  Time: {point.get('time')}, Value: {point.get('value')}")
            
            for point in dataset:
                time_str = point.get('time')
                value = point.get('value')
                if time_str and value is not None: # Check for None explicitly for value
                    timestamp = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
                    logger.info(f"Yielding heart_rate: {timestamp}, {value}")
                    yield ('heart_rate', timestamp, value)
        else:
            logger.warning("No 'activities-heart-intraday' key in heart_data response.")
            # Log the summary data if available
            if 'activities-heart' in heart_data:
                for activity in heart_data['activities-heart']:
                    logger.info(f"Summary data for {activity.get('dateTime')}:")
                    logger.info(f"- Resting heart rate: {activity.get('value', {}).get('restingHeartRate', 'N/A')}")
                    logger.info(f"- Heart rate zones: {activity.get('value', {}).get('heartRateZones', [])}")
            
            # Intentar con el endpoint alternativo
            alt_heart_rate_url = f"https://api.fitbit.com/1/user/-/activities/heart/date/{date_str}/1d/{detail_level}.json"
            logger.info(f"Trying alternative endpoint: {alt_heart_rate_url}")
            
            alt_response = requests.get(alt_heart_rate_url, headers=headers)
            if alt_response.status_code == 200:
                alt_heart_data = alt_response.json()
                logger.info("Alternative endpoint response:")
                logger.info(json.dumps(alt_heart_data, indent=2))
                
                if 'activities-heart-intraday' in alt_heart_data:
                    logger.info("Found intraday data in alternative endpoint!")
                    intraday_data = alt_heart_data['activities-heart-intraday']
                    dataset = intraday_data.get('dataset', [])
                    
                    for point in dataset:
                        time_str = point.get('time')
                        value = point.get('value')
                        if time_str and value is not None: # Check for None explicitly for value
                            timestamp = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
                            logger.info(f"Yielding alt_heart_rate: {timestamp}, {value}")
                            yield ('heart_rate', timestamp, value)

        # 2. PASOS INTRADÍA (Steps)
        steps_url = f"https://api.fitbit.com/1/user/-/activities/steps/date/{date_str}/1d/{detail_level}.json"
        logger.info(f"\n=== Steps API Request ===")
        logger.info(f"URL: {steps_url}")
        
        steps_response = requests.get(steps_url, headers=headers)
        logger.info(f"Status Code: {steps_response.status_code}")
        
        if steps_response.status_code == 200:
            steps_data = steps_response.json()
            logger.info("Response Body:")
            logger.info(json.dumps(steps_data, indent=2))
            
            if 'activities-steps-intraday' in steps_data:
                intraday_data = steps_data['activities-steps-intraday']
                dataset = intraday_data.get('dataset', [])
                logger.info(f"Puntos de datos de pasos: {len(dataset)}")
                
                for point in dataset:
                    time_str = point.get('time')
                    value = point.get('value')
                    if time_str and value is not None: # Check for None explicitly for value, as 0 is a valid value
                        timestamp = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
                        logger.info(f"Yielding steps: {timestamp}, {value}")
                        yield ('steps', timestamp, value)
            else:
                logger.warning("No 'activities-steps-intraday' key in steps_data response.")
        else:
            logger.error(f"Error getting steps data: {steps_response.status_code}")
            logger.error(f"Response: {steps_response.text}")

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            logger.warning("Token expired or invalid in get_intraday_data. Attempting refresh...")
            new_access_token = refresh_fitbit_token()
            if new_access_token:
                logger.info("Token refreshed. Retrying get_intraday_data logic with new token...")
                # Re-call itself effectively, and yield from that new call
                for item in get_intraday_data(new_access_token, date_str):
                    yield item
                return # Important to return after yielding from the recursive call
            else:
                logger.error("Failed to refresh token in get_intraday_data. Cannot proceed.")
                return # End the generator
        elif e.response.status_code == 429:
            logger.error("Rate limit exceeded. Waiting for reset...")
            reset_time = int(e.response.headers.get('fitbit-rate-limit-reset', 60))
            logger.info(f"Rate limit will reset in {reset_time} seconds")
            time.sleep(reset_time)
            return None
        logger.error(f"Error HTTP al obtener datos intradía: {e}")
        return None
    except Exception as e:
        logger.error(f"Error inesperado al obtener datos intradía: {e}")
        return None

def reset_tables():
    """Drop and recreate all relevant tables."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Drop tables if exist (order matters for foreign keys)
        cur.execute("DROP TABLE IF EXISTS alerts CASCADE;")
        cur.execute("DROP TABLE IF EXISTS sleep_logs CASCADE;")
        cur.execute("DROP TABLE IF EXISTS intraday_metrics CASCADE;")
        cur.execute("DROP TABLE IF EXISTS daily_summaries CASCADE;")
        cur.execute("DROP TABLE IF EXISTS users CASCADE;")
        conn.commit()
        logger.info("All tables dropped successfully.")
    except Exception as e:
        logger.error(f"Error dropping tables: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()
    # Recreate tables
    init_db()

def log_daily_summaries(user_id, start_date, end_date):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT * FROM daily_summaries
            WHERE user_id = %s AND date BETWEEN %s AND %s
            ORDER BY date ASC;
        """, (user_id, start_date, end_date))
        rows = cur.fetchall()
        logger.info(f"\n=== RAW daily_summaries for user_id={user_id} ===")
        for row in rows:
            logger.info(row)
    except Exception as e:
        logger.error(f"Error querying daily_summaries: {e}")
    finally:
        cur.close()
        conn.close()


def log_intraday_metrics(user_id, start_date, end_date):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Raw data
        cur.execute("""
            SELECT * FROM intraday_metrics
            WHERE user_id = %s AND time::date BETWEEN %s AND %s
            ORDER BY time ASC;
        """, (user_id, start_date, end_date))
        rows = cur.fetchall()
        logger.info(f"\n=== RAW intraday_metrics for user_id={user_id} ===")
        for row in rows:
            logger.info(row)
        # Aggregated by 5 minutes
        cur.execute("""
            SELECT type, date_trunc('minute', time) - INTERVAL '1 minute' * (EXTRACT(MINUTE FROM time)::int % 5) AS time_5min, AVG(value) as avg_value
            FROM intraday_metrics
            WHERE user_id = %s AND time::date BETWEEN %s AND %s
            GROUP BY type, time_5min
            ORDER BY type, time_5min ASC;
        """, (user_id, start_date, end_date))
        agg_rows = cur.fetchall()
        logger.info(f"\n=== intraday_metrics aggregated by 5min for user_id={user_id} ===")
        for row in agg_rows:
            logger.info(row)
    except Exception as e:
        logger.error(f"Error querying intraday_metrics: {e}")
    finally:
        cur.close()
        conn.close()

def collect_historical_data(days=3):
    """Collect historical data for the specified number of days for the CURRENT_USER_EMAIL."""
    global CURRENT_USER_CONFIG, CURRENT_USER_EMAIL
    if not CURRENT_USER_CONFIG or not CURRENT_USER_CONFIG.get('access_token'):
        logger.error(f"No access token available for {CURRENT_USER_EMAIL}. Cannot collect data.")
        return False
    if not CURRENT_USER_EMAIL:
        logger.error("No user email selected. Cannot collect data.")
        return False

    try:
        # The user record (including client_id, client_secret, tokens) should already be in the DB
        # from the initial sync or loaded by load_tokens_from_db.
        # We just need the user_id for data association.
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE email = %s", (CURRENT_USER_EMAIL,))
        user_record = cur.fetchone()
        if not user_record:
            logger.error(f"User {CURRENT_USER_EMAIL} not found in database. Cannot proceed.")
            cur.close()
            conn.close()
            return False
        user_id = user_record[0]
        # No need to commit here as we are just selecting. DB connection for data insertion is handled below.
        # cur.close() # Keep cursor open for data insertion
        # conn.close() # Keep connection open

        # Collect data for each day
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        current_date = start_date
   

        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            logger.info(f"Collecting data for {date_str}")

            try:
                # Get daily summary (commented out as it works correctly)
                """
                daily_data = get_fitbit_data(FITBIT_CONFIG['access_token'], date_str)
                if daily_data:
                    cur.execute('''
                        INSERT INTO daily_summaries (
                            user_id, date, steps, heart_rate, sleep_minutes,
                            calories, distance, floors, elevation, active_minutes,
                            sedentary_minutes, nutrition_calories, water, weight,
                            bmi, fat, oxygen_saturation, respiratory_rate, temperature
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (user_id, date) DO UPDATE SET
                            steps = EXCLUDED.steps,
                            heart_rate = EXCLUDED.heart_rate,
                            sleep_minutes = EXCLUDED.sleep_minutes,
                            calories = EXCLUDED.calories,
                            distance = EXCLUDED.distance,
                            floors = EXCLUDED.floors,
                            elevation = EXCLUDED.elevation,
                            active_minutes = EXCLUDED.active_minutes,
                            sedentary_minutes = EXCLUDED.sedentary_minutes,
                            nutrition_calories = EXCLUDED.nutrition_calories,
                            water = EXCLUDED.water,
                            weight = EXCLUDED.weight,
                            bmi = EXCLUDED.bmi,
                            fat = EXCLUDED.fat,
                            oxygen_saturation = EXCLUDED.oxygen_saturation,
                            respiratory_rate = EXCLUDED.respiratory_rate,
                            temperature = EXCLUDED.temperature;
                    ''', (
                        user_id, date_str,
                        daily_data['steps'],
                        daily_data['heart_rate'],
                        daily_data['sleep_minutes'],
                        daily_data['calories'],
                        daily_data['distance'],
                        daily_data['floors'],
                        daily_data['elevation'],
                        daily_data['active_minutes'],
                        daily_data['sedentary_minutes'],
                        daily_data['nutrition_calories'],
                        daily_data['water'],
                        daily_data.get('weight', 0),
                        daily_data.get('bmi', 0),
                        daily_data.get('fat', 0),
                        daily_data.get('spo2', 0),
                        daily_data.get('respiratory_rate', 0),
                        daily_data.get('temperature', 0)
                    ))
                    conn.commit()
                    logger.info(f"Daily data saved for {date_str}")
                """

                # Get and save intraday data with improved error handling
                try:
                    # Collect intraday data for the current processing date
                    intraday_data_points = 0
                    # Ensure we are using the potentially refreshed token from CURRENT_USER_CONFIG
                    current_access_token = CURRENT_USER_CONFIG['access_token']
                    intraday_generator = get_intraday_data(current_access_token, date_str)
                    
                    if intraday_generator:
                        logger.info(f"Iterating intraday_generator for {date_str}...")
                        for metric_type, timestamp, value in intraday_generator:
                            logger.info(f"Received from generator: {metric_type}, {timestamp}, {value} for user_id {user_id}")
                            try:
                                cur.execute("""
                                    INSERT INTO intraday_metrics (user_id, time, type, value)
                                    VALUES (%s, %s, %s, %s)
                                """, (user_id, timestamp, metric_type, value))
                                conn.commit()
                                intraday_data_points += 1
                                logger.info(f"Intraday data saved: {metric_type} at {timestamp} with value {value}")
                            except Exception as e:
                                logger.error(f"Error saving intraday data point: {e}")
                                logger.error(f"Failed data: metric_type={metric_type}, timestamp={timestamp}, value={value}")
                                conn.rollback()
                                continue
                        
                        logger.info(f"Total intraday data points saved for {date_str}: {intraday_data_points}")
                        
                        if intraday_data_points == 0:
                            logger.warning(f"No intraday data points were saved for {date_str}")
                            logger.warning("This might indicate an issue with the API response or data format if data was expected.")
                    # Removed the else block that skipped intraday data collection for other dates
                        
                except requests.exceptions.HTTPError as e:
                    # get_intraday_data now handles its own 401s.
                    # This block will catch other HTTP errors or if refresh failed and 401 is re-raised.
                    if e.response.status_code == 401:
                        logger.error(f"Authentication error (401) persisted for intraday data on {date_str} even after refresh attempt.")
                        # Potentially log more details or decide to stop. For now, loop continues.
                    elif e.response.status_code == 429:
                        logger.error("Authentication error (401) - Token might be expired")
                        raise  # Re-raise to handle token refresh
                    elif e.response.status_code == 429:
                        logger.error("Rate limit exceeded (429) - Need to implement backoff strategy")
                        time.sleep(60)  # Wait for 1 minute before retrying
                    else:
                        logger.error(f"HTTP error while getting intraday data: {e}")
                        logger.error(f"Response status: {e.response.status_code}")
                        logger.error(f"Response text: {e.response.text}")
                except Exception as e:
                    logger.error(f"Unexpected error while getting intraday data: {e}")
                    logger.error("Stack trace:", exc_info=True)

            except Exception as e:
                logger.error(f"Error collecting data for {date_str}: {e}")
                logger.error("Stack trace:", exc_info=True)
                conn.rollback()

            # Sleep to respect API rate limits
            time.sleep(1)  # 1 second between requests
            current_date += timedelta(days=1)

        # Log daily summaries for the period
        log_daily_summaries(user_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        # Log intraday metrics for the period
        log_intraday_metrics(user_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

        logger.info("Historical data collection completed")
        return True
    except Exception as e:
        logger.error(f"Error collecting historical data: {e}")
        logger.error("Stack trace:", exc_info=True)
        return False
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)

    # Reset tables to ensure schema is updated with client_id and client_secret columns
    # reset_tables()
    # init_db() will be called by reset_tables()

    # Synchronize all accounts from ACCOUNTS_CONFIG to the database.
    conn_sync = None
    try:
        conn_sync = get_db_connection()
        cur_sync = conn_sync.cursor()
        logger.info("Synchronizing all initial account configurations to database...")
        for email, config in ACCOUNTS_CONFIG.items():
            cur_sync.execute("""
                INSERT INTO users (email, client_id, client_secret, access_token, refresh_token)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (email) DO UPDATE SET
                    client_id = EXCLUDED.client_id,
                    client_secret = EXCLUDED.client_secret,
                    access_token = EXCLUDED.access_token,
                    refresh_token = EXCLUDED.refresh_token;
            """, (email, config['client_id'], config['client_secret'], config['access_token'], config['refresh_token']))
            logger.info(f"Synchronized initial config for {email}.")
        conn_sync.commit()
        cur_sync.close()
    except Exception as e:
        logger.error(f"Error synchronizing initial account configs to DB: {e}")
        if conn_sync:
            conn_sync.rollback()
    finally:
        if conn_sync:
            conn_sync.close()

    # Interactive menu to select user
    # global CURRENT_USER_EMAIL, CURRENT_USER_CONFIG # This is not needed at module level
    print("\nAvailable Fitbit Accounts:")
    account_emails = list(ACCOUNTS_CONFIG.keys())
    for i, email_option in enumerate(account_emails):
        print(f"{i + 1}. {email_option}")
    
    selected_index = -1
    while selected_index < 0 or selected_index >= len(account_emails):
        try:
            choice = input(f"Select account by number (1-{len(account_emails)}): ")
            selected_index = int(choice) - 1
            if selected_index < 0 or selected_index >= len(account_emails):
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number.")

    CURRENT_USER_EMAIL = account_emails[selected_index]
    logger.info(f"User selected: {CURRENT_USER_EMAIL}")

    # Load tokens for the selected user (this will also set CURRENT_USER_CONFIG)
    if not load_tokens_from_db(CURRENT_USER_EMAIL):
        # If DB load fails or no tokens, CURRENT_USER_CONFIG is already set to initial config by load_tokens_from_db
        logger.warning(f"Could not load full credentials for {CURRENT_USER_EMAIL} from DB, relying on initial config.")
        # Ensure CURRENT_USER_CONFIG is populated from ACCOUNTS_CONFIG if it's still None
        if CURRENT_USER_EMAIL in ACCOUNTS_CONFIG and CURRENT_USER_CONFIG is None:
             CURRENT_USER_CONFIG = ACCOUNTS_CONFIG[CURRENT_USER_EMAIL].copy()


    if CURRENT_USER_CONFIG:
        logger.info(f"Starting Fitbit personal data collection for {CURRENT_USER_EMAIL}")
        collect_historical_data(days=1) # Collect for yesterday and today
        logger.info(f"Finished Fitbit personal data collection for {CURRENT_USER_EMAIL}")
    else:
        logger.error(f"No configuration loaded for {CURRENT_USER_EMAIL}. Cannot proceed.")