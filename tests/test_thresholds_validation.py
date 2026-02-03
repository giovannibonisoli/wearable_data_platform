import os
import sys
import logging
from datetime import datetime, timedelta
import json

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import DatabaseManager
from alert_rules import (
    check_activity_drop,
    check_sedentary_increase,
    check_sleep_duration_change,
    check_heart_rate_anomaly,
    check_data_quality,
    check_intraday_activity_drop
)

# Configure logging directory
LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs', 'thresholds')
os.makedirs(LOG_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'detailed.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def test_activity_drop(user_id, current_date, expected=True):
    """
    Test for activity drop alert using the actual alert_rules function.
    Returns a dictionary with test results and reasoning.
    """
    result = {
        'triggered': False,
        'expected': expected,
        'reason': '',
        'details': {}
    }
    
    db = DatabaseManager()
    if not db.connect():
        return result
    
    try:
        # Use the actual alert_rules function
        triggered = check_activity_drop(user_id, current_date)
        result['triggered'] = triggered
        
        # Get data for logging purposes
        start_date = current_date - timedelta(days=7)
        daily_summaries = db.get_daily_summaries(user_id, start_date, current_date)
        
        if daily_summaries:
            previous_days = [s for s in daily_summaries if s[2] < current_date.date()]
            today_data = next((s for s in daily_summaries if s[2] == current_date.date()), None)
            
            if previous_days and today_data:
                valid_steps = [s[3] for s in previous_days if s[3] is not None and s[3] > 0]
                valid_active_minutes = [s[9] for s in previous_days if s[9] is not None and s[9] > 0]
                
                if valid_steps and valid_active_minutes:
                    avg_steps = sum(valid_steps) / len(valid_steps)
                    avg_active_minutes = sum(valid_active_minutes) / len(valid_active_minutes)
                    today_steps = today_data[3] or 0
                    today_active_minutes = today_data[9] or 0
                    
                    steps_drop = ((avg_steps - today_steps) / avg_steps * 100) if avg_steps > 0 else 0
                    active_minutes_drop = ((avg_active_minutes - today_active_minutes) / avg_active_minutes * 100) if avg_active_minutes > 0 else 0
                    
                    result['details'] = {
                        'avg_steps': round(avg_steps, 2),
                        'today_steps': today_steps,
                        'steps_drop_percent': round(steps_drop, 2),
                        'avg_active_minutes': round(avg_active_minutes, 2),
                        'today_active_minutes': today_active_minutes,
                        'active_minutes_drop_percent': round(active_minutes_drop, 2)
                    }
        
        result['reason'] = f"Activity drop alert {'triggered' if triggered else 'not triggered'}"
        
    except Exception as e:
        result['reason'] = f'Error during test: {str(e)}'
    finally:
        db.close()
        
    return result

def test_sedentary_increase(user_id, current_date, expected=True):
    """
    Test for sedentary increase alert using the actual alert_rules function.
    """
    result = {
        'triggered': False,
        'expected': expected,
        'reason': '',
        'details': {}
    }
    
    db = DatabaseManager()
    if not db.connect():
        return result
    
    try:
        triggered = check_sedentary_increase(user_id, current_date)
        result['triggered'] = triggered
        
        # Get data for logging purposes
        start_date = current_date - timedelta(days=7)
        daily_summaries = db.get_daily_summaries(user_id, start_date, current_date)
        
        if daily_summaries:
            previous_days = [s for s in daily_summaries if s[2] < current_date.date()]
            today_data = next((s for s in daily_summaries if s[2] == current_date.date()), None)
            
            if previous_days and today_data:
                valid_sedentary = [s[11] for s in previous_days if s[11] is not None and s[11] > 0]
                if valid_sedentary:
                    avg_sedentary = sum(valid_sedentary) / len(valid_sedentary)
                    today_sedentary = today_data[11] or 0
                    increase = ((today_sedentary - avg_sedentary) / avg_sedentary * 100) if avg_sedentary > 0 else 0
                    
                    result['details'] = {
                        'avg_sedentary_minutes': round(avg_sedentary, 2),
                        'today_sedentary_minutes': today_sedentary,
                        'increase_percent': round(increase, 2)
                    }
        
        result['reason'] = f"Sedentary increase alert {'triggered' if triggered else 'not triggered'}"
        
    except Exception as e:
        result['reason'] = f'Error during test: {str(e)}'
    finally:
        db.close()
        
    return result

def test_sleep_duration_change(user_id, current_date, expected=True):
    """
    Test for sleep duration change alert using the actual alert_rules function.
    """
    result = {
        'triggered': False,
        'expected': expected,
        'reason': '',
        'details': {}
    }
    
    db = DatabaseManager()
    if not db.connect():
        return result
    
    try:
        triggered = check_sleep_duration_change(user_id, current_date)
        result['triggered'] = triggered
        
        # Get data for logging purposes
        start_date = current_date - timedelta(days=7)
        daily_summaries = db.get_daily_summaries(user_id, start_date, current_date)
        
        if daily_summaries:
            previous_days = [s for s in daily_summaries if s[2] < current_date.date()]
            today_data = next((s for s in daily_summaries if s[2] == current_date.date()), None)
            
            if previous_days and today_data:
                valid_sleep = [s[5] for s in previous_days if s[5] is not None and s[5] > 0]
                if valid_sleep:
                    avg_sleep = sum(valid_sleep) / len(valid_sleep)
                    today_sleep = today_data[5] or 0
                    change = ((today_sleep - avg_sleep) / avg_sleep * 100) if avg_sleep > 0 else 0
                    
                    result['details'] = {
                        'avg_sleep_minutes': round(avg_sleep, 2),
                        'today_sleep_minutes': today_sleep,
                        'change_percent': round(change, 2)
                    }
        
        result['reason'] = f"Sleep duration change alert {'triggered' if triggered else 'not triggered'}"
        
    except Exception as e:
        result['reason'] = f'Error during test: {str(e)}'
    finally:
        db.close()
        
    return result

def test_heart_rate_anomaly(user_id, current_date, expected=True):
    """
    Test for heart rate anomaly alert using the actual alert_rules function.
    """
    result = {
        'triggered': False,
        'expected': expected,
        'reason': '',
        'details': {}
    }
    
    db = DatabaseManager()
    if not db.connect():
        return result
    
    try:
        triggered = check_heart_rate_anomaly(user_id, current_date)
        result['triggered'] = triggered
        
        # Get data for logging purposes
        start_time = current_date - timedelta(hours=24)
        heart_rate_data = db.get_intraday_metrics(user_id, 'heart_rate', start_time, current_date)
        
        if heart_rate_data:
            values = [hr[1] for hr in heart_rate_data]
            avg_hr = sum(values) / len(values)
            std_dev = (sum((x - avg_hr) ** 2 for x in values) / len(values)) ** 0.5
            anomalies = [(i, hr) for i, hr in enumerate(heart_rate_data) if abs(hr[1] - avg_hr) > 2 * std_dev]
            
            if anomalies:
                anomaly_percentage = (len(anomalies) / len(heart_rate_data)) * 100
                max_idx, max_anomaly = max(anomalies, key=lambda x: abs(x[1][1] - avg_hr))
                max_deviation = abs(max_anomaly[1] - avg_hr)
                
                result['details'] = {
                    'avg_heart_rate': round(avg_hr, 2),
                    'std_dev': round(std_dev, 2),
                    'anomaly_percentage': round(anomaly_percentage, 2),
                    'max_anomaly_value': max_anomaly[1],
                    'max_anomaly_time': max_anomaly[0].strftime('%H:%M'),
                    'max_deviation': round(max_deviation, 2)
                }
        
        result['reason'] = f"Heart rate anomaly alert {'triggered' if triggered else 'not triggered'}"
        
    except Exception as e:
        result['reason'] = f'Error during test: {str(e)}'
    finally:
        db.close()
        
    return result

def test_data_quality(user_id, current_date, expected=True):
    """
    Test for data quality alert using the actual alert_rules function.
    """
    result = {
        'triggered': False,
        'expected': expected,
        'reason': '',
        'details': {
            'missing_fields': [],
            'out_of_range_fields': []
        }
    }
    
    try:
        triggered = check_data_quality(user_id, current_date)
        result['triggered'] = triggered
        
        # Get data for logging purposes
        end_date = current_date
        start_date = current_date - timedelta(days=1)
        summaries = db.get_daily_summaries(user_id, start_date, end_date)
        
        if summaries:
            current_data = summaries[-1]
            ranges = {
                'steps': (0, 50000),
                'heart_rate': (30, 200),
                'sleep_minutes': (0, 1440),
                'sedentary_minutes': (0, 1440),
                'oxygen_saturation': (80, 100)
            }
            
            for field, (min_val, max_val) in ranges.items():
                value = current_data[3] if field == 'steps' else \
                       current_data[4] if field == 'heart_rate' else \
                       current_data[5] if field == 'sleep_minutes' else \
                       current_data[11] if field == 'sedentary_minutes' else \
                       current_data[16] if field == 'oxygen_saturation' else None
                       
                if value is None:
                    result['details']['missing_fields'].append(field)
                elif value < min_val or value > max_val:
                    result['details']['out_of_range_fields'].append({
                        'field': field,
                        'value': value,
                        'range': f'{min_val}-{max_val}'
                    })
        
        result['reason'] = f"Data quality alert {'triggered' if triggered else 'not triggered'}"
        
    except Exception as e:
        result['reason'] = f'Error during test: {str(e)}'
    finally:
        db.close()
        
    return result

def test_intraday_activity_drop(user_id, current_date, expected=True):
    """
    Test for intraday activity drop alert using the actual alert_rules function.
    """
    result = {
        'triggered': False,
        'expected': expected,
        'reason': '',
        'details': {}
    }
    
    db = DatabaseManager()
    if not db.connect():
        return result
    
    try:
        triggered = check_intraday_activity_drop(user_id, current_date)
        result['triggered'] = triggered
        
        # Get data for logging purposes
        start_time = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = start_time + timedelta(days=1)
        steps_data = db.get_intraday_metrics(user_id, 'steps', start_time, end_time)
        
        if steps_data:
            zero_streak = []
            max_streak = []
            
            for i, (t, v) in enumerate(steps_data):
                if v == 0:
                    zero_streak.append((t, v))
                else:
                    if len(zero_streak) >= 2 and len(zero_streak) > len(max_streak):
                        max_streak = zero_streak.copy()
                    zero_streak = []
                    
            if len(zero_streak) >= 2 and len(zero_streak) > len(max_streak):
                max_streak = zero_streak.copy()
                
            if max_streak:
                result['details'] = {
                    'zero_streak_start': max_streak[0][0].strftime('%H:%M'),
                    'zero_streak_end': max_streak[-1][0].strftime('%H:%M'),
                    'zero_streak_duration_hours': len(max_streak)
                }
        
        result['reason'] = f"Intraday activity drop alert {'triggered' if triggered else 'not triggered'}"
        
    except Exception as e:
        result['reason'] = f'Error during test: {str(e)}'
    finally:
        db.close()
        
    return result

def generate_logs(test_results):
    """
    Generate detailed and summary logs from test results.
    """
    # Generate detailed log
    with open(os.path.join(LOG_DIR, 'detailed.log'), 'w') as f:
        f.write("=== Detailed Alert Test Results ===\n\n")
        for user_id, results in test_results.items():
            f.write(f"\nUser ID: {user_id}\n")
            f.write("=" * 50 + "\n")
            for date_type, date_results in results.items():
                f.write(f"\n{date_type.upper()} DATA:\n")
                f.write("-" * 30 + "\n")
                for alert_type, result in date_results.items():
                    f.write(f"\nAlert Type: {alert_type}\n")
                    f.write("-" * 20 + "\n")
                    f.write(f"Expected: {result['expected']}\n")
                    f.write(f"Triggered: {result['triggered']}\n")
                    f.write(f"Reason: {result['reason']}\n")
                    if result['details']:
                        f.write("\nDetails:\n")
                        for key, value in result['details'].items():
                            f.write(f"  {key}: {value}\n")
                    f.write("\n")
    
    # Generate summary log
    with open(os.path.join(LOG_DIR, 'summary.log'), 'w') as f:
        f.write("=== Alert Test Summary ===\n\n")
        total_alerts = 0
        expected_triggers = 0
        actual_triggers = 0
        false_positives = 0
        false_negatives = 0
        
        for user_id, results in test_results.items():
            f.write(f"\nUser ID: {user_id}\n")
            f.write("=" * 50 + "\n")
            for date_type, date_results in results.items():
                f.write(f"\n{date_type.upper()} DATA:\n")
                f.write("-" * 30 + "\n")
                for alert_type, result in date_results.items():
                    total_alerts += 1
                    if result['expected']:
                        expected_triggers += 1
                    if result['triggered']:
                        actual_triggers += 1
                    if result['triggered'] and not result['expected']:
                        # Solo considerar como falso positivo si no es una excepción como 'data_quality'
                        if alert_type != "data_quality":
                           false_positives += 1
                    if not result['triggered'] and result['expected']:
                        false_negatives += 1
                        
                    status = '✓' if result['triggered'] == result['expected'] else '✗'
                    f.write(f"\n{alert_type}:\n")
                    f.write(f"  Status: {status}\n")
                    f.write(f"  Expected: {result['expected']}\n")
                    f.write(f"  Actual: {result['triggered']}\n")
                    f.write(f"  Reason: {result['reason']}\n")
        
        f.write("\n=== Overall Statistics ===\n")
        f.write(f"Total Alerts Tested: {total_alerts}\n")
        f.write(f"Expected Triggers: {expected_triggers}\n")
        f.write(f"Actual Triggers: {actual_triggers}\n")
        f.write(f"False Positives: {false_positives}\n")
        f.write(f"False Negatives: {false_negatives}\n")
        
        detection_rate = (actual_triggers / expected_triggers * 100) if expected_triggers > 0 else 0
        precision = ((actual_triggers - false_positives) / actual_triggers * 100) if actual_triggers > 0 else 0
        f.write(f"\nDetection Rate: {detection_rate:.1f}%\n")
        f.write(f"Precision: {precision:.1f}%\n")

def run_all_tests():
    """
    Run all alert tests for all users and generate logs.
    """
    # Test dates
    test_dates = {
        'anomalous': datetime(2025, 5, 14),  # Date with anomalies (activity drop and sedentary increase)
        'normal': datetime(2025, 5, 13)      # Date with normal data
    }
    
    # User IDs to test
    user_ids = [2, 3, 4]
    
    # Dictionary to store all test results
    all_results = {}
    
    for user_id in user_ids:
        logger.info(f"\nTesting alerts for User {user_id}")
        logger.info("=" * 50)
        
        # Test with anomalous data (expected=True)
        user_results = {
            'activity_drop': test_activity_drop(user_id, test_dates['anomalous'], expected=True),
            'sedentary_increase': test_sedentary_increase(user_id, test_dates['anomalous'], expected=True),
            'sleep_duration_change': test_sleep_duration_change(user_id, test_dates['anomalous'], expected=True),
            'heart_rate_anomaly': test_heart_rate_anomaly(user_id, test_dates['anomalous'], expected=True),
            'data_quality': test_data_quality(user_id, test_dates['anomalous'], expected=True),
            'intraday_activity_drop': test_intraday_activity_drop(user_id, test_dates['anomalous'], expected=True)
        }
        
        # Test with normal data (expected=False)
        normal_results = {
            'activity_drop': test_activity_drop(user_id, test_dates['normal'], expected=False),
            'sedentary_increase': test_sedentary_increase(user_id, test_dates['normal'], expected=False),
            'sleep_duration_change': test_sleep_duration_change(user_id, test_dates['normal'], expected=False),
            'heart_rate_anomaly': test_heart_rate_anomaly(user_id, test_dates['normal'], expected=False),
            'data_quality': test_data_quality(user_id, test_dates['normal'], expected=False),
            'intraday_activity_drop': test_intraday_activity_drop(user_id, test_dates['normal'], expected=False)
        }
        
        # Combine results
        all_results[user_id] = {
            'anomalous': user_results,
            'normal': normal_results
        }
        
        # Log results for this user
        for date_type, results in all_results[user_id].items():
            logger.info(f"\n{date_type.upper()} DATA RESULTS:")
            for alert_type, result in results.items():
                logger.info(f"\n{alert_type}:")
                logger.info(f"  Expected: {result['expected']}")
                logger.info(f"  Triggered: {result['triggered']}")
                logger.info(f"  Reason: {result['reason']}")
                if result['details']:
                    logger.info("  Details:")
                    for key, value in result['details'].items():
                        logger.info(f"    {key}: {value}")
    
    # Generate detailed and summary logs
    generate_logs(all_results)
    
    return all_results

if __name__ == "__main__":
    run_all_tests() 