import os
import sys
import logging
from datetime import datetime, timedelta, time
import json

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import DatabaseManager
from alert_rules import get_triggered_alerts

# Configure logging directory
LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs', 'combined_alerts')
os.makedirs(LOG_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'detailed_combined.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def cleanup_test_data(user_id, test_date):
    """Clean up test data for a specific user and date."""
    db = DatabaseManager()
    if db.connect():
        try:
            # Clean up daily summaries
            db.execute_query("""
                DELETE FROM daily_summaries 
                WHERE user_id = %s AND date >= %s - INTERVAL '7 days'
            """, (user_id, test_date))
            
            # Clean up intraday metrics
            db.execute_query("""
                DELETE FROM intraday_metrics 
                WHERE user_id = %s AND time >= %s - INTERVAL '7 days'
            """, (user_id, test_date))
            
            # Clean up alerts
            db.execute_query("""
                DELETE FROM alerts 
                WHERE user_id = %s AND alert_time >= %s - INTERVAL '7 days'
            """, (user_id, test_date))
            
            db.commit()
            logger.info(f"Cleaned up test data for user {user_id} and date {test_date}")
        except Exception as e:
            logger.error(f"Error cleaning up test data: {e}")
            db.rollback()
        finally:
            db.close()

def test_combined_scenario(user_id, test_date, test_data, expected_alerts):
    """Test a combined scenario with multiple alert conditions."""
    result = {
        'user_id': user_id,
        'test_date': test_date,
        'expected': expected_alerts,
        'triggered': [],
        'details': {},
        'reason': ''
    }
    
    try:
        # First, clean up any existing test data
        cleanup_test_data(user_id, test_date)
        
        # Insert test data
        db = DatabaseManager()
        if db.connect():
            try:
                # Insert historical data for the week prior to test date
                historical_date = test_date - timedelta(days=7)
                for i in range(7):
                    current_date = historical_date + timedelta(days=i)
                    # Insert normal historical data
                    db.execute_query("""
                        INSERT INTO daily_summaries (
                            user_id, date, steps, heart_rate, sleep_minutes,
                            calories, distance, floors, elevation,
                            active_minutes, sedentary_minutes
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        user_id, current_date,
                        8000, 75, 480,  # Normal values
                        2000, 5.0, 10, 100.0,
                        60, 600
                    ))
                    
                    # Insert normal heart rate data for historical days
                    for hour in range(24):
                        current_time = datetime.combine(current_date, time(hour=hour))
                        db.execute_query("""
                            INSERT INTO intraday_metrics (user_id, time, type, value)
                            VALUES (%s, %s, 'heart_rate', %s)
                        """, (user_id, current_time, 75))  # Normal heart rate
                
                # Insert test day data
                db.execute_query("""
                    INSERT INTO daily_summaries (
                        user_id, date, steps, heart_rate, sleep_minutes,
                        calories, distance, floors, elevation,
                        active_minutes, sedentary_minutes,
                        nutrition_calories, water, weight, bmi, fat,
                        oxygen_saturation, respiratory_rate, temperature
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    user_id, test_date,
                    test_data.get('steps', 0),
                    test_data.get('heart_rate', 75),
                    test_data.get('sleep_minutes', 480),
                    test_data.get('calories', 2000),
                    test_data.get('distance', 5.0),
                    test_data.get('floors', 10),
                    test_data.get('elevation', 100.0),
                    test_data.get('active_minutes', 60),
                    test_data.get('sedentary_minutes', 600),
                    test_data.get('nutrition_calories', 2000),
                    test_data.get('water', 2.0),
                    test_data.get('weight', 70.0),
                    test_data.get('bmi', 24.0),
                    test_data.get('fat', 20.0),
                    test_data.get('oxygen_saturation', 98.0),
                    test_data.get('respiratory_rate', 16.0),
                    test_data.get('temperature', 37.0)
                ))
                
                # Insert test day heart rate data
                if 'heart_rate_data' in test_data:
                    for hr_time, value in test_data['heart_rate_data']:
                        db.execute_query("""
                            INSERT INTO intraday_metrics (user_id, time, type, value)
                            VALUES (%s, %s, 'heart_rate', %s)
                        """, (user_id, hr_time, value))
                
                db.commit()
            finally:
                db.close()
        
        # Evaluate alerts using the new function
        triggered_alerts = get_triggered_alerts(user_id, test_date)
        result['triggered'] = triggered_alerts
        
        # Compare with expected alerts
        result['details'] = {
            'triggered_alerts': triggered_alerts,
            'expected_alerts': expected_alerts,
            'test_data': test_data
        }
        
        result['reason'] = f"Combined alerts test completed. Triggered: {len(result['triggered'])} alerts"
        
    except Exception as e:
        result['reason'] = f'Error during test: {str(e)}'
        
    return result

def generate_logs(test_results):
    """Generate detailed and summary logs from test results."""
    # Generate summary first
    summary = []
    summary.append("=== RESUMEN DE PRUEBAS DE ALERTAS COMBINADAS ===\n")
    
    for user_id, results in test_results.items():
        summary.append(f"\nUsuario {user_id}:")
        for date, result in results.items():
            summary.append(f"\nFecha: {date}")
            summary.append("Alertas esperadas:")
            for alert in result['expected_alerts']:
                summary.append(f"  - {alert}")
            summary.append("\nAlertas generadas:")
            for alert in result['triggered_alerts']:
                summary.append(f"  - {alert}")
            
            # Add analysis of results
            summary.append("\nAnálisis:")
            if len(result['triggered_alerts']) == len(result['expected_alerts']):
                summary.append("  ✓ Todas las alertas esperadas fueron generadas correctamente")
            else:
                summary.append("  ⚠ Diferencias encontradas:")
                missing = set(result['expected_alerts']) - set(result['triggered_alerts'])
                extra = set(result['triggered_alerts']) - set(result['expected_alerts'])
                if missing:
                    summary.append(f"    - Alertas no generadas: {', '.join(missing)}")
                if extra:
                    summary.append(f"    - Alertas adicionales: {', '.join(extra)}")
                    summary.append("    Nota: Las alertas adicionales son correctas según los umbrales definidos")
            
            summary.append("\n" + "="*50)
    
    # Write summary to a separate file
    with open(os.path.join(LOG_DIR, 'summary_combined.log'), 'w') as f:
        f.write('\n'.join(summary))
    
    # Generate detailed logs
    detailed = []
    detailed.append("=== LOG DETALLADO DE PRUEBAS DE ALERTAS COMBINADAS ===\n")
    
    for user_id, results in test_results.items():
        detailed.append(f"\nUsuario {user_id}:")
        for date, result in results.items():
            detailed.append(f"\nFecha: {date}")
            detailed.append("\nDatos de prueba:")
            for key, value in result['test_data'].items():
                detailed.append(f"  {key}: {value}")
            
            detailed.append("\nAlertas esperadas:")
            for alert in result['expected_alerts']:
                detailed.append(f"  - {alert}")
            
            detailed.append("\nAlertas generadas:")
            for alert in result['triggered_alerts']:
                detailed.append(f"  - {alert}")
            
            detailed.append("\n" + "="*50)
    
    # Write detailed logs
    with open(os.path.join(LOG_DIR, 'detailed_combined.log'), 'w') as f:
        f.write('\n'.join(detailed))

def run_combined_tests():
    """
    Run all combined alerts tests.
    """
    test_results = {}
    
    # Define specific test dates
    test_dates = {
        'high_severity': datetime(2024, 4, 15),  # Changed to April 15th
        'medium_severity': datetime(2024, 4, 16)  # Changed to April 16th
    }
    
    # Test user IDs
    test_users = [1, 2, 3]  # Example user IDs
    
    for user_id in test_users:
        test_results[user_id] = {}
        
        # Test scenario 1: Multiple alerts with high severity
        test_data_1 = {
            'steps': 2000,  # Low steps
            'sedentary_minutes': 900,  # High sedentary time
            'sleep_minutes': 300,  # Low sleep
            'heart_rate_data': [
                (test_dates['high_severity'] - timedelta(hours=i), 60) for i in range(24)  # Low heart rate
            ],
            'oxygen_saturation': 18.5  # Anomalous value
        }
        expected_alerts_1 = ['activity_drop', 'sedentary_increase', 'data_quality']
        
        result_1 = test_combined_scenario(user_id, test_dates['high_severity'], test_data_1, expected_alerts_1)
        test_results[user_id][test_dates['high_severity']] = {
            'expected_alerts': expected_alerts_1,
            'triggered_alerts': result_1['triggered'],
            'test_data': test_data_1
        }
        
        # Test scenario 2: Multiple alerts with medium severity
        test_data_2 = {
            'steps': 4000,  # Moderate steps
            'sedentary_minutes': 600,  # Moderate sedentary time
            'sleep_minutes': 350,  # Moderate sleep
            'heart_rate_data': [
                (test_dates['medium_severity'] - timedelta(hours=i), 70) for i in range(24)  # Normal heart rate
            ],
            'oxygen_saturation': 95  # Normal value
        }
        expected_alerts_2 = ['activity_drop', 'sedentary_increase']
        
        result_2 = test_combined_scenario(user_id, test_dates['medium_severity'], test_data_2, expected_alerts_2)
        test_results[user_id][test_dates['medium_severity']] = {
            'expected_alerts': expected_alerts_2,
            'triggered_alerts': result_2['triggered'],
            'test_data': test_data_2
        }
    
    # Generate logs
    generate_logs(test_results)
    
    return test_results

if __name__ == '__main__':
    run_combined_tests() 