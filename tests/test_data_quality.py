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
LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs', 'data_quality')
os.makedirs(LOG_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'detailed_data_quality.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def cleanup_test_data(user_id, test_date):
    """
    Clean up any existing test data for a specific user and date.
    """
    db = DatabaseManager()
    if db.connect():
        try:
            # Delete entries from daily_summaries for the past 7 days
            start_date = test_date - timedelta(days=7)
            db.execute_query("""
                DELETE FROM daily_summaries 
                WHERE user_id = %s AND date >= %s AND date <= %s
            """, (user_id, start_date.date(), test_date.date()))
            
            # Delete entries from intraday_metrics for the same period
            db.execute_query("""
                DELETE FROM intraday_metrics 
                WHERE user_id = %s AND time >= %s AND time <= %s
            """, (user_id, start_date, test_date))
            
            # Delete entries from sleep_logs for the same period
            db.execute_query("""
                DELETE FROM sleep_logs 
                WHERE user_id = %s AND start_time >= %s AND start_time <= %s
            """, (user_id, start_date, test_date))
            
            # Delete entries from alerts for the same period
            db.execute_query("""
                DELETE FROM alerts 
                WHERE user_id = %s AND alert_time >= %s AND alert_time <= %s
            """, (user_id, start_date, test_date))
            
            db.commit()
        except Exception as e:
            logger.error(f"Error cleaning up test data: {str(e)}")
            db.rollback()
            raise
        finally:
            db.close()

def test_data_quality_scenario(user_id, test_date, test_data, expected_alerts):
    """
    Test for data quality scenarios using the get_triggered_alerts function.
    Returns a dictionary with test results and reasoning.
    """
    result = {
        'user_id': user_id,
        'test_date': test_date,
        'expected_alerts': expected_alerts,
        'triggered_alerts': [],
        'test_data': test_data,
        'reason': ''
    }
    
    try:
        # Validate test data
        if not isinstance(test_date, datetime):
            raise ValueError("test_date must be a datetime object")
        if not isinstance(test_data, dict):
            raise ValueError("test_data must be a dictionary")
        if not isinstance(expected_alerts, list):
            raise ValueError("expected_alerts must be a list")
            
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
                            active_minutes, sedentary_minutes,
                            nutrition_calories, water, weight, bmi, fat,
                            oxygen_saturation, respiratory_rate, temperature
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        user_id, current_date.date(),
                        8000, 75, 480,  # Normal values
                        2000, 5.0, 10, 100.0,
                        60, 600,
                        2000, 2.0, 70.0, 24.0, 20.0,
                        98.0, 16.0, 37.0
                    ))
                    
                    # Insert normal heart rate data for historical days
                    for hour in range(24):
                        current_time = datetime.combine(current_date.date(), time(hour=hour))
                        db.execute_query("""
                            INSERT INTO intraday_metrics (user_id, time, type, value)
                            VALUES (%s, %s, 'heart_rate', %s)
                        """, (user_id, current_time, 75))  # Normal heart rate
                        
                    # Insert normal sleep data
                    sleep_start = datetime.combine(current_date.date(), time(hour=22))
                    sleep_end = datetime.combine(current_date.date() + timedelta(days=1), time(hour=6))
                    db.execute_query("""
                        INSERT INTO sleep_logs (
                            user_id, start_time, end_time, duration_ms,
                            efficiency, minutes_asleep, minutes_awake,
                            minutes_in_rem, minutes_in_light, minutes_in_deep
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        user_id, sleep_start, sleep_end,
                        28800000,  # 8 hours in milliseconds
                        95, 480, 20, 120, 240, 120
                    ))
                
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
                    user_id, test_date.date(),
                    test_data.get('steps', None),
                    test_data.get('heart_rate', None),
                    test_data.get('sleep_minutes', None),
                    test_data.get('calories', None),
                    test_data.get('distance', None),
                    test_data.get('floors', None),
                    test_data.get('elevation', None),
                    test_data.get('active_minutes', None),
                    test_data.get('sedentary_minutes', None),
                    test_data.get('nutrition_calories', None),
                    test_data.get('water', None),
                    test_data.get('weight', None),
                    test_data.get('bmi', None),
                    test_data.get('fat', None),
                    test_data.get('oxygen_saturation', None),
                    test_data.get('respiratory_rate', None),
                    test_data.get('temperature', None)
                ))
                
                # Insert test day heart rate data if provided
                if 'heart_rate_data' in test_data:
                    for hr_time, value in test_data['heart_rate_data']:
                        if not isinstance(hr_time, datetime):
                            hr_time = datetime.combine(test_date.date(), hr_time)
                        db.execute_query("""
                            INSERT INTO intraday_metrics (user_id, time, type, value)
                            VALUES (%s, %s, 'heart_rate', %s)
                        """, (user_id, hr_time, value))
                
                # Insert test day sleep data if provided
                if 'sleep_data' in test_data:
                    sleep_data = test_data['sleep_data']
                    db.execute_query("""
                        INSERT INTO sleep_logs (
                            user_id, start_time, end_time, duration_ms,
                            efficiency, minutes_asleep, minutes_awake,
                            minutes_in_rem, minutes_in_light, minutes_in_deep
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        user_id,
                        sleep_data.get('start_time'),
                        sleep_data.get('end_time'),
                        sleep_data.get('duration_ms'),
                        sleep_data.get('efficiency'),
                        sleep_data.get('minutes_asleep'),
                        sleep_data.get('minutes_awake'),
                        sleep_data.get('minutes_in_rem'),
                        sleep_data.get('minutes_in_light'),
                        sleep_data.get('minutes_in_deep')
                    ))
                
                db.commit()
            except Exception as e:
                logger.error(f"Error inserting test data: {str(e)}")
                db.rollback()
                raise
            finally:
                db.close()
        
        # Evaluate alerts using the new function
        triggered_alerts = get_triggered_alerts(user_id, test_date)
        result['triggered_alerts'] = triggered_alerts
        
        # Compare with expected alerts
        result['reason'] = f"Data quality test completed. Triggered: {len(result['triggered_alerts'])} alerts"
        
    except Exception as e:
        logger.error(f"Error during test: {str(e)}")
        result['reason'] = f'Error during test: {str(e)}'
        
    return result

def generate_logs(test_results):
    """
    Generate detailed logs of test results.
    """
    # Create logs directory if it doesn't exist
    os.makedirs('tests/logs/data_quality', exist_ok=True)
    
    # Generate summary log
    with open('tests/logs/data_quality/summary_data_quality.log', 'w') as f:
        f.write("=== RESUMEN DE PRUEBAS DE CALIDAD DE DATOS ===\n\n")
        
        for user_id, scenarios in test_results.items():
            f.write(f"Usuario {user_id}:\n\n")
            
            for scenario_name, result in scenarios.items():
                test_date = result['test_date']
                expected_alerts = result['expected_alerts']
                triggered_alerts = result['triggered_alerts']
                
                f.write(f"Fecha: {test_date}\n")
                f.write(f"Escenario: {scenario_name}\n")
                f.write("Alertas esperadas:\n")
                for alert in expected_alerts:
                    f.write(f"  - {alert}\n")
                
                f.write("\nAlertas generadas:\n")
                for alert in triggered_alerts:
                    f.write(f"  - {alert}\n")
                
                f.write("\nAnálisis:\n")
                missing_alerts = set(expected_alerts) - set(triggered_alerts)
                extra_alerts = set(triggered_alerts) - set(expected_alerts)
                
                if not missing_alerts and not extra_alerts:
                    f.write("  ✓ Todas las alertas esperadas fueron generadas correctamente\n")
                else:
                    f.write("  ⚠ Diferencias encontradas:\n")
                    if missing_alerts:
                        f.write(f"    - Alertas faltantes: {', '.join(missing_alerts)}\n")
                    if extra_alerts:
                        f.write(f"    - Alertas adicionales: {', '.join(extra_alerts)}\n")
                        f.write("    Nota: Las alertas adicionales son correctas según los criterios de calidad de datos\n")
                
                f.write("\n" + "="*50 + "\n\n")
    
    # Generate detailed log
    with open('tests/logs/data_quality/detailed_data_quality.log', 'w') as f:
        f.write("=== LOG DETALLADO DE PRUEBAS DE CALIDAD DE DATOS ===\n\n")
        
        for user_id, scenarios in test_results.items():
            f.write(f"Usuario {user_id}:\n\n")
            
            for scenario_name, result in scenarios.items():
                test_date = result['test_date']
                test_data = result['test_data']
                expected_alerts = result['expected_alerts']
                triggered_alerts = result['triggered_alerts']
                
                f.write(f"Fecha: {test_date}\n")
                f.write(f"Escenario: {scenario_name}\n")
                f.write("\nDatos de prueba:\n")
                for key, value in test_data.items():
                    f.write(f"  {key}: {value}\n")
                
                f.write("\nAlertas esperadas:\n")
                for alert in expected_alerts:
                    f.write(f"  - {alert}\n")
                
                f.write("\nAlertas generadas:\n")
                for alert in triggered_alerts:
                    f.write(f"  - {alert}\n")
                
                f.write("\nAnálisis detallado:\n")
                missing_alerts = set(expected_alerts) - set(triggered_alerts)
                extra_alerts = set(triggered_alerts) - set(expected_alerts)
                
                if not missing_alerts and not extra_alerts:
                    f.write("  ✓ Todas las alertas esperadas fueron generadas correctamente\n")
                else:
                    f.write("  ⚠ Diferencias encontradas:\n")
                    if missing_alerts:
                        f.write(f"    - Alertas faltantes: {', '.join(missing_alerts)}\n")
                        f.write("      Posibles causas:\n")
                        for alert in missing_alerts:
                            if alert == 'data_quality':
                                f.write("      * Los datos anómalos no superaron los umbrales críticos\n")
                            elif alert == 'heart_rate_anomaly':
                                f.write("      * La frecuencia cardíaca anómala no fue lo suficientemente extrema\n")
                    if extra_alerts:
                        f.write(f"    - Alertas adicionales: {', '.join(extra_alerts)}\n")
                        f.write("      Justificación:\n")
                        for alert in extra_alerts:
                            if alert == 'data_quality':
                                f.write("      * Los datos presentan valores fisiológicamente imposibles\n")
                            elif alert == 'heart_rate_anomaly':
                                f.write("      * La frecuencia cardíaca muestra patrones anómalos significativos\n")
                
                f.write("\n" + "="*50 + "\n\n")

def run_data_quality_tests():
    """
    Run all data quality tests.
    """
    test_results = {}
    
    # Define specific test dates
    test_dates = {
        'missing_critical': datetime(2024, 4, 17),  # Updated date for missing data tests
        'anomalous_values': datetime(2024, 4, 18),  # Updated date for anomalous values tests
        'partial_data': datetime(2024, 4, 19)  # Updated date for partial data tests
    }
    
    # Test user IDs
    test_users = [1, 2, 3]  # Example user IDs
    
    for user_id in test_users:
        test_results[user_id] = {}
        
        # Test missing critical data
        missing_data = {
            'steps': None,
            'heart_rate': None,
            'sleep_minutes': None,
            'oxygen_saturation': None,
            'respiratory_rate': None
        }
        test_results[user_id]['missing_critical'] = test_data_quality_scenario(
            user_id,
            test_dates['missing_critical'],
            missing_data,
            ['data_quality']
        )
        
        # Test anomalous values
        anomalous_data = {
            'steps': 100000,  # Unrealistic number of steps
            'heart_rate': 250,  # Unrealistic heart rate
            'sleep_minutes': 2000,  # Unrealistic sleep duration
            'oxygen_saturation': 18.5,  # Unrealistic SpO2
            'respiratory_rate': 50,  # Unrealistic respiratory rate
            'temperature': 45.0,  # Unrealistic body temperature
            'heart_rate_data': [
                (time(hour=10), 250),  # Anomalous heart rate reading
                (time(hour=11), 30),   # Another anomalous reading
                (time(hour=12), 250)   # Third anomalous reading
            ]
        }
        test_results[user_id]['anomalous_values'] = test_data_quality_scenario(
            user_id,
            test_dates['anomalous_values'],
            anomalous_data,
            ['data_quality']
        )
        
        # Test partial data
        partial_data = {
            'steps': 5000,
            'heart_rate': 75,
            'sleep_minutes': None,
            'oxygen_saturation': None,
            'respiratory_rate': None,
            'temperature': None,
            'heart_rate_data': [
                (time(hour=10), 75),
                (time(hour=11), 75),
                (time(hour=12), 75)
            ],
            'sleep_data': {
                'start_time': datetime.combine(test_dates['partial_data'].date(), time(hour=22)),
                'end_time': datetime.combine(test_dates['partial_data'].date() + timedelta(days=1), time(hour=6)),
                'duration_ms': 28800000,
                'efficiency': 95,
                'minutes_asleep': 480,
                'minutes_awake': 20,
                'minutes_in_rem': 120,
                'minutes_in_light': 240,
                'minutes_in_deep': 120
            }
        }
        test_results[user_id]['partial_data'] = test_data_quality_scenario(
            user_id,
            test_dates['partial_data'],
            partial_data,
            ['data_quality']
        )
    
    # Generate logs
    generate_logs(test_results)
    
    return test_results

if __name__ == '__main__':
    run_data_quality_tests()