#!/usr/bin/env python3
"""
Example usage of the refactored database system.

This demonstrates both the old (facade) and new (repository) approaches.
"""

from datetime import datetime, date, timedelta
from database import (
    ConnectionManager,
    Database,
    AdminUserRepository,
    DeviceRepository,
    MetricsRepository,
    SleepRepository,
    AlertRepository
)


def example_old_way():
    """
    Example using the Database facade (backward compatible with old code).
    """
    print("=" * 60)
    print("OLD WAY - Using Database Facade")
    print("=" * 60)
    
    db = Database()
    if not db.connect():
        print("Failed to connect to database")
        return
    
    try:
        # Authenticate admin user
        user = db.verify_admin_user("admin_username", "password123")
        if user:
            print(f"\n✓ Authenticated: {user['full_name']}")
            
            # Get admin's devices
            devices = db.get_admin_user_devices(user['id'])
            if devices:
                print(f"✓ Found {len(devices)} devices")
                for device_id, email, status, device_type in devices:
                    print(f"  - {email} ({status})")
        
        # Get daily summaries
        summaries = db.get_daily_summaries(
            device_id=1,
            start_date=date.today() - timedelta(days=7),
            end_date=date.today()
        )
        print(f"\n✓ Retrieved {len(summaries)} daily summaries")
        
    finally:
        db.close()


def example_new_way():
    """
    Example using repositories directly (recommended for new code).
    """
    print("\n" + "=" * 60)
    print("NEW WAY - Using Repositories")
    print("=" * 60)
    
    with ConnectionManager() as db:
        # Initialize repositories
        admin_repo = AdminUserRepository(db)
        device_repo = DeviceRepository(db)
        metrics_repo = MetricsRepository(db)
        sleep_repo = SleepRepository(db)
        alert_repo = AlertRepository(db)
        
        # Authenticate admin user
        user = admin_repo.verify_credentials("admin_username", "password123")
        if user:
            print(f"\n✓ Authenticated: {user.full_name}")
            print(f"  ID: {user.id}")
            print(f"  Username: {user.username}")
            print(f"  Email: {user.email}")
            
            # Get admin's devices
            devices = device_repo.get_by_admin_user(user.id)
            print(f"\n✓ Found {len(devices)} devices:")
            for device in devices:
                print(f"  - {device.email_address}")
                print(f"    Status: {device.authorization_status}")
                print(f"    Type: {device.device_type}")
                print(f"    Last sync: {device.last_synch}")
        
        # Work with metrics
        print("\n" + "-" * 60)
        print("Working with Health Metrics")
        print("-" * 60)
        
        # Get recent daily summaries
        summaries = metrics_repo.get_daily_summaries(
            device_id=1,
            start_date=date.today() - timedelta(days=7)
        )
        
        print(f"\n✓ Retrieved {len(summaries)} daily summaries:")
        for summary in summaries[:3]:  # Show first 3
            print(f"  {summary.date}:")
            print(f"    Steps: {summary.steps}")
            print(f"    Heart Rate: {summary.heart_rate}")
            print(f"    Calories: {summary.calories}")
        
        # Insert a new daily summary
        success = metrics_repo.insert_daily_summary(
            device_id=1,
            date_value=date.today(),
            steps=12500,
            heart_rate=68.5,
            calories=2800.0,
            distance=10.2,
            active_minutes=90
        )
        print(f"\n✓ Inserted new summary: {success}")
        
        # Work with intraday data
        heart_rates = metrics_repo.get_intraday_metrics(
            device_id=1,
            metric_type='heart_rate',
            start_time=datetime.now() - timedelta(hours=2)
        )
        print(f"\n✓ Retrieved {len(heart_rates)} heart rate measurements")
        
        # Work with sleep data
        print("\n" + "-" * 60)
        print("Working with Sleep Data")
        print("-" * 60)
        
        sleep_logs = sleep_repo.get_sleep_logs(
            device_id=1,
            start_date=datetime.now() - timedelta(days=7)
        )
        
        print(f"\n✓ Retrieved {len(sleep_logs)} sleep logs:")
        for log in sleep_logs[:2]:  # Show first 2
            print(f"  {log.start_time.date()}:")
            print(f"    Duration: {log.duration / 3600:.1f} hours")
            print(f"    Asleep: {log.minutes_asleep} minutes")
            print(f"    Main sleep: {log.is_main_sleep}")
        
        # Work with alerts
        print("\n" + "-" * 60)
        print("Working with Alerts")
        print("-" * 60)
        
        # Create a new alert
        alert_id = alert_repo.create(
            email_id=1,
            alert_type="heart_rate_high",
            priority="medium",
            triggering_value=165.0,
            threshold=160.0,
            details="Heart rate spike during exercise"
        )
        print(f"\n✓ Created new alert: ID {alert_id}")
        
        # Get unacknowledged alerts
        unack_alerts = alert_repo.get_alerts(email_id=1, acknowledged=False)
        print(f"\n✓ Unacknowledged alerts: {len(unack_alerts)}")
        
        # Get count of unacknowledged
        count = alert_repo.get_unacknowledged_count(email_id=1)
        print(f"✓ Total unacknowledged count: {count}")


def example_device_management():
    """
    Example of device management operations.
    """
    print("\n" + "=" * 60)
    print("Device Management Example")
    print("=" * 60)
    
    with ConnectionManager() as db:
        device_repo = DeviceRepository(db)
        
        # Create a new device
        device_id = device_repo.create(
            admin_user_id=1,
            email_address="user@example.com",
            access_token="encrypted_access_token_here",
            refresh_token="encrypted_refresh_token_here"
        )
        print(f"\n✓ Created new device: ID {device_id}")
        
        # Update device status
        device_repo.update_status(device_id, 'authorized')
        print(f"✓ Updated device status to 'authorized'")
        
        # Update device type
        device_repo.update_device_type(device_id, 'fitbit')
        print(f"✓ Set device type to 'fitbit'")
        
        # Get device details
        device = device_repo.get_by_id(device_id)
        if device:
            print(f"\n✓ Retrieved device:")
            print(f"  Email: {device.email_address}")
            print(f"  Status: {device.authorization_status}")
            print(f"  Type: {device.device_type}")
        
        # Update sync checkpoint
        device_repo.update_last_synch(device_id, datetime.now())
        device_repo.update_daily_summaries_checkpoint(device_id, date.today())
        print(f"\n✓ Updated sync checkpoints")
        
        # Get all authorized devices
        authorized = device_repo.get_all_authorized()
        print(f"\n✓ Total authorized devices: {len(authorized)}")


def example_comparison():
    """
    Side-by-side comparison of old vs new approach.
    """
    print("\n" + "=" * 60)
    print("COMPARISON - Same Operation, Different Approaches")
    print("=" * 60)
    
    # OLD WAY
    print("\n[OLD] Getting admin user and their devices:")
    db_old = Database()
    if db_old.connect():
        user_dict = db_old.verify_admin_user("admin", "pass")
        if user_dict:
            devices_tuples = db_old.get_admin_user_devices(user_dict['id'])
            print(f"  User: {user_dict['full_name']}")
            print(f"  Devices: {len(devices_tuples) if devices_tuples else 0}")
        db_old.close()
    
    # NEW WAY
    print("\n[NEW] Getting admin user and their devices:")
    with ConnectionManager() as db_new:
        admin_repo = AdminUserRepository(db_new)
        device_repo = DeviceRepository(db_new)
        
        user_obj = admin_repo.verify_credentials("admin", "pass")
        if user_obj:
            devices_list = device_repo.get_by_admin_user(user_obj.id)
            print(f"  User: {user_obj.full_name}")
            print(f"  Devices: {len(devices_list)}")
            
            # Type-safe access (IDE autocomplete works!)
            for device in devices_list:
                print(f"    - {device.email_address} ({device.authorization_status})")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("DATABASE REFACTORING - USAGE EXAMPLES")
    print("=" * 60)
    
    # Uncomment the examples you want to run:
    
    # example_old_way()
    # example_new_way()
    # example_device_management()
    # example_comparison()
    
    print("\n✓ Examples complete!")
    print("\nTo run examples, uncomment the function calls in __main__")
