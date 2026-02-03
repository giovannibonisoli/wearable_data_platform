"""
Example test file demonstrating how to use the mock database.

This file shows different ways to use MockDatabaseManager in your tests.
"""

import os
import sys
from datetime import datetime, timedelta

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.mock_db import MockDatabaseManager
from tests.test_helpers import use_mock_database, MockDatabaseContext, get_mock_db


# Method 1: Direct usage of MockDatabaseManager
def test_direct_mock_usage():
    """Example: Using MockDatabaseManager directly."""
    print("\n=== Test 1: Direct Mock Usage ===")
    
    # Create a mock database instance
    db = MockDatabaseManager()
    db.connect()
    
    # Add a user
    user_id = db.add_user("Test User", "test@example.com")
    print(f"Created user with ID: {user_id}")
    
    # Insert daily summary
    today = datetime.now().date()
    db.insert_daily_summary(
        email_id=user_id,
        date=today,
        steps=10000,
        heart_rate=75,
        sleep_minutes=420
    )
    
    # Retrieve data
    summaries = db.get_daily_summaries(user_id)
    print(f"Retrieved {len(summaries)} daily summaries")
    if summaries:
        print(f"First summary: {summaries[0]}")
    
    # Insert intraday metric
    timestamp = datetime.now().replace(hour=10, minute=0, second=0, microsecond=0)
    db.insert_intraday_metric(
        email_id=user_id,
        timestamp=timestamp,
        data_type='heart_rate',
        value=72
    )
    
    # Retrieve intraday metrics
    metrics = db.get_intraday_metrics(user_id, 'heart_rate')
    print(f"Retrieved {len(metrics)} intraday metrics")
    
    db.close()
    print("✅ Test 1 passed\n")


# Method 2: Using the decorator to patch DatabaseManager
@use_mock_database
def test_with_decorator():
    """Example: Using the decorator to automatically use mock database."""
    print("\n=== Test 2: Using Decorator ===")
    
    # Import DatabaseManager - it will be automatically replaced with MockDatabaseManager
    from db import DatabaseManager
    
    db = DatabaseManager()
    db.connect()
    
    user_id = db.add_user("Decorator User", "decorator@test.com")
    print(f"Created user with ID: {user_id}")
    
    # Insert and retrieve data
    db.insert_daily_summary(
        email_id=user_id,
        date=datetime.now().date(),
        steps=5000,
        heart_rate=80
    )
    
    summaries = db.get_daily_summaries(user_id)
    print(f"Retrieved {len(summaries)} summaries")
    
    db.close()
    print("✅ Test 2 passed\n")


# Method 3: Using context manager
def test_with_context_manager():
    """Example: Using context manager for temporary mock database."""
    print("\n=== Test 3: Using Context Manager ===")
    
    with MockDatabaseContext():
        from db import DatabaseManager
        
        db = DatabaseManager()  # This is actually MockDatabaseManager
        db.connect()
        
        user_id = db.add_user("Context User", "context@test.com")
        print(f"Created user with ID: {user_id}")
        
        # Insert alert
        alert_id = db.insert_alert(
            email_id=user_id,
            alert_type='activity_drop',
            priority='high',
            triggering_value=50.0,
            threshold='30%',
            details='Test alert'
        )
        print(f"Created alert with ID: {alert_id}")
        
        # Retrieve alerts
        alerts = db.get_user_alerts(user_id)
        print(f"Retrieved {len(alerts)} alerts")
        
        db.close()
    
    print("✅ Test 3 passed\n")


# Method 4: Testing with multiple users and complex scenarios
def test_complex_scenario():
    """Example: Testing a more complex scenario with multiple users."""
    print("\n=== Test 4: Complex Scenario ===")
    
    db = MockDatabaseManager()
    db.connect()
    
    # Create multiple users
    users = [
        ("User A", "user_a@test.com"),
        ("User B", "user_b@test.com"),
        ("User C", "user_c@test.com")
    ]
    
    user_ids = []
    for name, email in users:
        user_id = db.add_user(name, email)
        user_ids.append(user_id)
        print(f"Created {name} with ID {user_id}")
    
    # Insert data for each user
    base_date = datetime(2025, 5, 1).date()
    for i, user_id in enumerate(user_ids):
        for day in range(7):
            date = base_date + timedelta(days=day)
            db.insert_daily_summary(
                email_id=user_id,
                date=date,
                steps=10000 + (i * 1000),
                heart_rate=70 + i,
                sleep_minutes=420
            )
            
            # Insert intraday data
            for hour in range(8, 20):
                timestamp = datetime.combine(date, datetime.min.time()).replace(hour=hour)
                db.insert_intraday_metric(
                    email_id=user_id,
                    timestamp=timestamp,
                    data_type='heart_rate',
                    value=70 + i + (hour % 10)
                )
    
    # Verify data
    for user_id in user_ids:
        summaries = db.get_daily_summaries(user_id, start_date=base_date)
        metrics = db.get_intraday_metrics(user_id, 'heart_rate')
        print(f"User {user_id}: {len(summaries)} summaries, {len(metrics)} metrics")
    
    # Test reset functionality
    all_data_before = db.get_all_data()
    print(f"Data before reset: {len(all_data_before['daily_summaries'])} summaries")
    
    db.reset()
    all_data_after = db.get_all_data()
    print(f"Data after reset: {len(all_data_after['daily_summaries'])} summaries")
    
    db.close()
    print("✅ Test 4 passed\n")


# Method 5: Testing alert functionality
def test_alert_functionality():
    """Example: Testing alert insertion and retrieval."""
    print("\n=== Test 5: Alert Functionality ===")
    
    db = MockDatabaseManager()
    db.connect()
    
    user_id = db.add_user("Alert User", "alert@test.com")
    
    # Insert multiple alerts
    alert_types = [
        ('activity_drop', 'high', 50.0, '30%'),
        ('sleep_duration_change', 'medium', 25.0, '20%'),
        ('heart_rate_anomaly', 'high', 120.0, '2 std dev'),
    ]
    
    alert_ids = []
    for alert_type, priority, value, threshold in alert_types:
        alert_id = db.insert_alert(
            email_id=user_id,
            alert_type=alert_type,
            priority=priority,
            triggering_value=value,
            threshold=threshold
        )
        alert_ids.append(alert_id)
    
    print(f"Created {len(alert_ids)} alerts")
    
    # Get all alerts
    all_alerts = db.get_user_alerts(user_id)
    print(f"Retrieved {len(all_alerts)} alerts")
    
    # Get only high priority alerts
    high_priority = db.get_user_alerts(user_id, acknowledged=None)
    high_priority_filtered = [a for a in all_alerts if a[4] == 'high']
    print(f"High priority alerts: {len(high_priority_filtered)}")
    
    # Get unacknowledged alerts
    unacknowledged = db.get_user_alerts(user_id, acknowledged=False)
    print(f"Unacknowledged alerts: {len(unacknowledged)}")
    
    db.close()
    print("✅ Test 5 passed\n")


if __name__ == "__main__":
    print("=" * 60)
    print("Mock Database Test Examples")
    print("=" * 60)
    
    try:
        test_direct_mock_usage()
        test_with_decorator()
        test_with_context_manager()
        test_complex_scenario()
        test_alert_functionality()
        
        print("=" * 60)
        print("All tests passed! ✅")
        print("=" * 60)
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
