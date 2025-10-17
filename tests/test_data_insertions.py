import os
import sys
import logging
from datetime import datetime, timedelta
import numpy as np

# Add the project root to the Python path so that we can import from db and alert_rules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import DatabaseManager, reset_database, init_db

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def insert_test_data():
    """Insert test data for alert rule validation.
    
    The test data is structured to trigger each alert type at least once for each user:
    
    1. Activity Drop Alert (May 14):
       - Steps: 10,000 → 2,000 (80% drop)
       - Active minutes: 60 → 15 (75% drop)
       - Both exceed 50% threshold for high priority
    
    2. Sedentary Increase Alert (May 14):
       - Sedentary minutes: 480 → 900 (87.5% increase)
       - Exceeds 50% threshold for high priority
    
    3. Sleep Duration Change Alert (May 15):
       - Sleep minutes: 420 → 240 (43% reduction)
       - Exceeds 30% threshold for high priority
    
    4. Heart Rate Anomaly Alert (May 15):
       - Normal HR (75 ± 1) for first 8 hours
       - Anomalous HR (130+) for last 4 hours
       - Exceeds 2 standard deviations threshold
    
    5. Data Quality Alert (May 16):
       - Heart rate: NULL (missing data)
       - Oxygen saturation: 120% (out of range)
       - Both trigger data quality alerts
    
    6. Intraday Activity Drop Alert (May 16):
       - First 4 hours: normal activity (500 + variation)
       - Last 8 hours: zero steps
       - Exceeds 2-hour threshold
    """
    # Reset and initialize database
    reset_database()
    init_db()
    
    # Create test users
    users = [
        ("User A", "user_a@test.com"),
        ("User B", "user_b@test.com"),
        ("User C", "user_c@test.com")
    ]
    
    db = DatabaseManager()
    if not db.connect():
        logger.error("Failed to connect to database")
        return
    
    user_ids = []
    for name, email in users:
        user_id = db.add_user(name, email)
        user_ids.append(user_id)
        logger.info(f"Created {name} with ID {user_id}")
    
    db.close()
    
    # Base date for test data - 20 days before the test date
    base_date = datetime(2025, 4, 26)
    
    # Insert 20 days of normal data to establish baseline
    logger.info("Inserting 20 days of normal data to establish baseline...")
    for i in range(20):
        date = base_date + timedelta(days=i)
        for user_id in user_ids:
            # Insert daily summary with normal values
            db.insert_daily_summary(
                email_id=user_id,
                date=date,
                steps=11000,  # Increased from 10000 to make drop more significant
                heart_rate=75,  # Normal heart rate
                sleep_minutes=420,  # 7 hours of sleep
                calories=2000,
                distance=8.5,
                floors=10,
                elevation=100.5,
                active_minutes=90,  # Increased from 60 to make drop more significant
                sedentary_minutes=400,  # Reduced from 480 to make increase more significant
                nutrition_calories=1800,
                water=2.5,
                weight=70.5,
                bmi=22.5,
                fat=18.5,
                oxygen_saturation=98.0,  # Normal oxygen saturation
                respiratory_rate=16.5,
                temperature=36.5
            )
            
            # Insert normal intraday heart rate data
            for hour in range(8, 20):  # 8 AM to 8 PM
                timestamp = date.replace(hour=hour)
                # Very low variance for normal heart rate (75 ± 1)
                db.insert_intraday_metric(
                    email_id=user_id,
                    timestamp=timestamp,
                    data_type='heart_rate',
                    value=75 + np.random.uniform(-1, 1)  # Normal heart rate with minimal variation
                )
                
                # Insert normal intraday steps
                db.insert_intraday_metric(
                    email_id=user_id,
                    timestamp=timestamp,
                    data_type='steps',
                    value=500 + (hour % 3) * 100  # Normal step count
                )
            
            # Insert normal sleep log
            sleep_start = date.replace(hour=22, minute=0)
            sleep_end = (date + timedelta(days=1)).replace(hour=6, minute=0)
            db.insert_sleep_log(
                email_id=user_id,
                start_time=sleep_start,
                end_time=sleep_end,
                duration_ms=8 * 60 * 60 * 1000,  # 8 hours
                efficiency=90,
                minutes_asleep=420,
                minutes_awake=30,
                minutes_in_rem=90,
                minutes_in_light=240,
                minutes_in_deep=90
            )
    
    # Insert 3 days of anomalous data (May 14-16, 2025)
    logger.info("Inserting 3 days of anomalous data to trigger alerts...")
    anomalous_dates = [
        datetime(2025, 5, 14),  # Activity drop and sedentary increase
        datetime(2025, 5, 15),  # Sleep duration change and heart rate anomaly
        datetime(2025, 5, 16)   # Data quality and intraday activity drop
    ]
    
    for i, current_date in enumerate(anomalous_dates):
        for user_id in user_ids:  # Apply anomalies to all users
            if i == 0:  # May 14: Activity drop and sedentary increase
                logger.info(f"Inserting data for May 14 to trigger activity drop and sedentary increase alerts for user {user_id}...")
db.insert_daily_summary(
                email_id=user_id,
                    date=current_date,
                    steps=2000,  # 82% drop from normal (11000)
                    heart_rate=90,
                    sleep_minutes=300,  # 28.6% drop from normal (420)
                    calories=1200,
                    distance=1.5,
                    floors=2,
                    elevation=20.5,
                    active_minutes=15,  # 83% drop from normal (90)
                    sedentary_minutes=900,  # 125% increase from normal (400)
                    nutrition_calories=1800,
                    water=2.0,
                    weight=70.5,
                    bmi=22.5,
                    fat=18.5,
                    oxygen_saturation=98.0,
                    respiratory_rate=16.5,
                    temperature=36.5
                )
                
                # Insert intraday data for May 14
                logger.info(f"Inserting intraday data for May 14 to trigger intraday activity drop alert for user {user_id}...")
                for hour in range(24):
                    if hour < 8:  # 8 consecutive hours of zero steps
                        steps = 0
                        heart_rate = 60
                        active_minutes = 0
                        calories = 0
                        distance = 0
                        floors = 0
                        elevation = 0
                    else:
                        steps = 500
                        heart_rate = 75
                        active_minutes = 30
                        calories = 100
                        distance = 0.4
                        floors = 1
                        elevation = 10
                    
db.insert_intraday_metric(
                    email_id=user_id,
                        timestamp=current_date.replace(hour=hour),
                        data_type='steps',
                        value=steps
                    )
db.insert_intraday_metric(
                    email_id=user_id,
                        timestamp=current_date.replace(hour=hour),
                        data_type='heart_rate',
                        value=heart_rate
                    )
db.insert_intraday_metric(
                    email_id=user_id,
                        timestamp=current_date.replace(hour=hour),
                        data_type='active_minutes',
                        value=active_minutes
                    )
db.insert_intraday_metric(
                    email_id=user_id,
                        timestamp=current_date.replace(hour=hour),
                        data_type='calories',
                        value=calories
                    )
db.insert_intraday_metric(
                    email_id=user_id,
                        timestamp=current_date.replace(hour=hour),
                        data_type='distance',
                        value=distance
                    )
db.insert_intraday_metric(
                    email_id=user_id,
                        timestamp=current_date.replace(hour=hour),
                        data_type='floors',
                        value=floors
                    )
db.insert_intraday_metric(
                    email_id=user_id,
                        timestamp=current_date.replace(hour=hour),
                        data_type='elevation',
                        value=elevation
                    )
            
            elif i == 1:  # May 15: Sleep duration change and heart rate anomaly
                logger.info(f"Inserting data for May 15 to trigger sleep duration change and heart rate anomaly alerts for user {user_id}...")
db.insert_daily_summary(
                email_id=user_id,
                    date=current_date,
                    steps=500,  # Changed from 0 to 500 to avoid data quality alert
                    heart_rate=75,
                    sleep_minutes=240,  # 43% reduction from normal (420)
                    calories=2000,
                    distance=8.5,
                    floors=10,
                    elevation=100.5,
                    active_minutes=60,
                    sedentary_minutes=480,
                    nutrition_calories=1800,
                    water=2.5,
                    weight=70.5,
                    bmi=22.5,
                    fat=18.5,
                    oxygen_saturation=98.0,
                    respiratory_rate=16.5,
                    temperature=36.5
                )
                
                # Insert anomalous heart rate data with very low variance for normal period
                for hour in range(8, 20):
                    timestamp = current_date.replace(hour=hour)
                    if hour < 16:  # Normal values for first 8 hours with minimal variance
                        value = 75 + np.random.uniform(-1, 1)  # Very low variance
                    else:  # Anomalous values for last 4 hours
                        value = 130 + np.random.uniform(-2, 2)  # Elevated heart rate (>2 std dev)
db.insert_intraday_metric(
                    email_id=user_id,
                        timestamp=timestamp,
                        data_type='heart_rate',
                        value=value
                    )
                
                # Insert anomalous sleep log
                sleep_start = current_date.replace(hour=23, minute=0)
                sleep_end = (current_date + timedelta(days=1)).replace(hour=3, minute=0)
db.insert_sleep_log(
                email_id=user_id,
                    start_time=sleep_start,
                    end_time=sleep_end,
                    duration_ms=4 * 60 * 60 * 1000,  # 4 hours
                    efficiency=85,
                    minutes_asleep=240,
                    minutes_awake=20,
                    minutes_in_rem=60,
                    minutes_in_light=120,
                    minutes_in_deep=60
                )
            
            else:  # May 16: Data quality and intraday activity drop
                logger.info(f"Inserting data for May 16 to trigger data quality and intraday activity drop alerts for user {user_id}...")
db.insert_daily_summary(
                email_id=user_id,
                    date=current_date,
                    steps=10000,
                    heart_rate=None,  # Missing data to trigger data quality alert
                    sleep_minutes=240,
                    calories=2000,
                    distance=8.5,
                    floors=10,
                    elevation=100.5,
                    active_minutes=60,
                    sedentary_minutes=480,
                    nutrition_calories=1800,
                    water=2.5,
                    weight=70.5,
                    bmi=22.5,
                    fat=18.5,
                    oxygen_saturation=120,  # Out of range to trigger data quality alert
                    respiratory_rate=16.5,
                    temperature=36.5
                )
                
                # Insert intraday steps data for activity drop detection
                for hour in range(8, 20):
                    timestamp = current_date.replace(hour=hour)
                    if hour < 12:  # First 4 hours: normal activity
                        value = 500 + (hour % 3) * 100  # Normal step count
                    else:  # Last 8 hours: zero steps to trigger intraday activity drop alert
                        value = 0
db.insert_intraday_metric(
                    email_id=user_id,
                        timestamp=timestamp,
                        data_type='steps',
                        value=value
                    )
    
    logger.info("Test data insertion completed successfully")

if __name__ == "__main__":
    insert_test_data() 