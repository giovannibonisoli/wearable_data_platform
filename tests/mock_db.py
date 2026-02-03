"""
Mock Database Manager for testing without a real database.

This module provides a MockDatabaseManager class that mimics the DatabaseManager
interface but stores all data in memory. This allows tests to run without requiring
a real PostgreSQL database connection.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import copy


class MockDatabaseManager:
    """
    In-memory mock database that mimics DatabaseManager behavior.
    
    Stores all data in dictionaries and lists, allowing tests to run
    without a real database connection.
    """
    
    def __init__(self):
        """Initialize the mock database with empty data structures."""
        self.connection = None
        self.cursor = None
        self._connected = False
        
        # In-memory data storage
        self._admin_users = []
        self._email_addresses = []
        self._daily_summaries = []
        self._intraday_metrics = []
        self._sleep_logs = []
        self._alerts = []
        self._pending_authorizations = []
        
        # Auto-increment counters
        self._admin_user_id_counter = 1
        self._email_id_counter = 1
        self._alert_id_counter = 1
        self._pending_auth_id_counter = 1
    
    def connect(self):
        """Simulate database connection."""
        self._connected = True
        return True
    
    def close(self):
        """Simulate closing database connection."""
        self._connected = False
        self.connection = None
        self.cursor = None
    
    def commit(self):
        """Simulate commit (no-op for in-memory storage)."""
        pass
    
    def rollback(self):
        """Simulate rollback (no-op for in-memory storage)."""
        pass
    
    def execute_query(self, query, params=None):
        """
        Execute a query. This is a simplified mock that handles basic operations.
        For complex queries, use the specific methods instead.
        """
        if not self._connected:
            return None
        
        # This is a basic implementation - specific methods should be used for better control
        return True
    
    def execute_many(self, query, params_list):
        """Execute a query multiple times with different parameters."""
        if not self._connected:
            return False
        return True
    
    # User/Email Management Methods
    
    def add_user(self, name, email):
        """
        Add a user (creates an email address entry).
        This method is used in tests but may not exist in the real DatabaseManager.
        """
        if not self._connected:
            return None
        
        email_id = self._email_id_counter
        self._email_id_counter += 1
        
        email_entry = {
            'id': email_id,
            'address_name': email,
            'name': name,
            'status': 'inserted',
            'admin_user_id': None,
            'access_token': None,
            'refresh_token': None,
            'device_type': None,
            'daily_summaries_checkpoint': None,
            'intraday_checkpoint': None,
            'last_synch': None,
            'created_at': datetime.now()
        }
        self._email_addresses.append(email_entry)
        return email_id
    
    def add_email_address(self, admin_user_id, address_name, access_token=None, refresh_token=None):
        """Add a new email address to the database."""
        if not self._connected:
            return None
        
        email_id = self._email_id_counter
        self._email_id_counter += 1
        
        email_entry = {
            'id': email_id,
            'address_name': address_name,
            'status': 'inserted',
            'admin_user_id': admin_user_id,
            'access_token': access_token,
            'refresh_token': refresh_token,
            'device_type': None,
            'daily_summaries_checkpoint': None,
            'intraday_checkpoint': None,
            'last_synch': None,
            'created_at': datetime.now()
        }
        self._email_addresses.append(email_entry)
        return email_id
    
    def get_email_id_by_name(self, address_name):
        """Retrieve email address id by its name."""
        for email in self._email_addresses:
            if email['address_name'] == address_name:
                return email['id']
        return None
    
    # Daily Summaries Methods
    
    def insert_daily_summary(self, email_id, date, **data):
        """Insert or update a daily summary."""
        if not self._connected:
            return None
        
        # Convert date to date object if it's a datetime
        if isinstance(date, datetime):
            date = date.date()
        
        # Check if entry exists
        existing = None
        for i, summary in enumerate(self._daily_summaries):
            if summary['email_id'] == email_id and summary['date'] == date:
                existing = i
                break
        
        summary_data = {
            'email_id': email_id,
            'date': date,
            'steps': data.get('steps'),
            'heart_rate': data.get('heart_rate'),
            'sleep_minutes': data.get('sleep_minutes'),
            'calories': data.get('calories'),
            'distance': data.get('distance'),
            'floors': data.get('floors'),
            'elevation': data.get('elevation'),
            'active_minutes': data.get('active_minutes'),
            'sedentary_minutes': data.get('sedentary_minutes'),
            'nutrition_calories': data.get('nutrition_calories'),
            'water': data.get('water'),
            'weight': data.get('weight'),
            'bmi': data.get('bmi'),
            'fat': data.get('fat'),
            'oxygen_saturation': data.get('oxygen_saturation'),
            'respiratory_rate': data.get('respiratory_rate'),
            'temperature': data.get('temperature')
        }
        
        if existing is not None:
            # Update existing
            self._daily_summaries[existing].update(summary_data)
        else:
            # Insert new
            self._daily_summaries.append(summary_data)
        
        return True
    
    def get_daily_summaries(self, email_id, start_date=None, end_date=None):
        """Get daily summaries for a user within a date range."""
        if not self._connected:
            return []
        
        results = []
        for summary in self._daily_summaries:
            if summary['email_id'] != email_id:
                continue
            
            date = summary['date']
            if start_date and date < start_date.date() if isinstance(start_date, datetime) else start_date:
                continue
            if end_date and date > end_date.date() if isinstance(end_date, datetime) else end_date:
                continue
            
            # Convert to tuple format similar to database results
            results.append((
                summary.get('id'),
                summary['email_id'],
                summary['date'],
                summary.get('steps'),
                summary.get('heart_rate'),
                summary.get('sleep_minutes'),
                summary.get('calories'),
                summary.get('distance'),
                summary.get('floors'),
                summary.get('elevation'),
                summary.get('active_minutes'),
                summary.get('sedentary_minutes'),
                summary.get('nutrition_calories'),
                summary.get('water'),
                summary.get('weight'),
                summary.get('bmi'),
                summary.get('fat'),
                summary.get('oxygen_saturation'),
                summary.get('respiratory_rate'),
                summary.get('temperature')
            ))
        
        return sorted(results, key=lambda x: x[2])  # Sort by date
    
    def get_daily_summary_checkpoint(self, email_id):
        """Get the last date for which a daily summary was collected."""
        summaries = [s for s in self._daily_summaries if s['email_id'] == email_id]
        if not summaries:
            return None
        return max(s['date'] for s in summaries)
    
    # Intraday Metrics Methods
    
    def check_intraday_timestamp(self, email_id, timestamp):
        """Check if intraday timestamp is already present."""
        for metric in self._intraday_metrics:
            if metric['email_id'] == email_id and metric['time'] == timestamp:
                return True
        return False
    
    def insert_intraday_metric(self, email_id, timestamp, data_type='heart_rate', value=None):
        """Insert intraday data into the database."""
        if not self._connected:
            return None
        
        # Check if entry exists
        existing = None
        for i, metric in enumerate(self._intraday_metrics):
            if metric['email_id'] == email_id and metric['time'] == timestamp:
                existing = i
                break
        
        if existing is not None:
            # Update existing record
            self._intraday_metrics[existing][data_type] = value
        else:
            # Insert new record
            metric_data = {
                'email_id': email_id,
                'time': timestamp,
                'heart_rate': None,
                'steps': None,
                'calories': None,
                'distance': None,
                'floors': None,
                'elevation': None,
                'active_minutes': None
            }
            metric_data[data_type] = value
            self._intraday_metrics.append(metric_data)
        
        return True
    
    def get_intraday_metrics(self, email_id, metric_type, start_time=None, end_time=None):
        """Get intraday metrics for a user."""
        if not self._connected:
            return []
        
        results = []
        for metric in self._intraday_metrics:
            if metric['email_id'] != email_id:
                continue
            
            time = metric['time']
            if start_time and time < start_time:
                continue
            if end_time and time > end_time:
                continue
            
            value = metric.get(metric_type)
            if value is not None:
                results.append((time, value))
        
        return sorted(results, key=lambda x: x[0])  # Sort by time
    
    def get_intraday_checkpoint(self, email_id):
        """Get the last date for which intraday data was collected."""
        metrics = [m for m in self._intraday_metrics if m['email_id'] == email_id]
        if not metrics:
            return None
        return max(m['time'] for m in metrics)
    
    # Sleep Logs Methods
    
    def insert_sleep_log(self, email_id, start_time, end_time, **data):
        """Insert a sleep record into the database."""
        if not self._connected:
            return None
        
        sleep_log = {
            'email_id': email_id,
            'start_time': start_time,
            'end_time': end_time,
            'duration_ms': data.get('duration_ms'),
            'efficiency': data.get('efficiency'),
            'minutes_asleep': data.get('minutes_asleep'),
            'minutes_awake': data.get('minutes_awake'),
            'minutes_in_rem': data.get('minutes_in_rem'),
            'minutes_in_light': data.get('minutes_in_light'),
            'minutes_in_deep': data.get('minutes_in_deep')
        }
        self._sleep_logs.append(sleep_log)
        return True
    
    def get_sleep_logs(self, email_id, start_date=None, end_date=None):
        """Get sleep records for a user."""
        if not self._connected:
            return []
        
        results = []
        for log in self._sleep_logs:
            if log['email_id'] != email_id:
                continue
            
            start_time = log['start_time']
            if start_date and start_time < start_date:
                continue
            if end_date and start_time > end_date:
                continue
            
            # Convert to tuple format
            results.append((
                log.get('id'),
                log['email_id'],
                log['start_time'],
                log['end_time'],
                log.get('duration_ms'),
                log.get('efficiency'),
                log.get('minutes_asleep'),
                log.get('minutes_awake'),
                log.get('minutes_in_rem'),
                log.get('minutes_in_light'),
                log.get('minutes_in_deep')
            ))
        
        return sorted(results, key=lambda x: x[2], reverse=True)  # Sort by start_time DESC
    
    # Alerts Methods
    
    def insert_alert(self, email_id, alert_type, priority, triggering_value, threshold, timestamp=None, details=None):
        """Insert a new alert into the database."""
        if not self._connected:
            return None
        
        if timestamp is None:
            timestamp = datetime.now()
        
        alert_id = self._alert_id_counter
        self._alert_id_counter += 1
        
        alert = {
            'id': alert_id,
            'email_id': email_id,
            'alert_type': alert_type,
            'priority': priority,
            'triggering_value': triggering_value,
            'threshold_value': str(threshold),
            'alert_time': timestamp,
            'details': details,
            'acknowledged': False,
            'acknowledged_at': None,
            'acknowledged_by': None
        }
        self._alerts.append(alert)
        return alert_id
    
    def get_user_alerts(self, email_id, start_time=None, end_time=None, acknowledged=None):
        """Get alerts for a user."""
        if not self._connected:
            return []
        
        results = []
        for alert in self._alerts:
            if alert['email_id'] != email_id:
                continue
            
            alert_time = alert['alert_time']
            if start_time and alert_time < start_time:
                continue
            if end_time and alert_time > end_time:
                continue
            if acknowledged is not None and alert['acknowledged'] != acknowledged:
                continue
            
            # Convert to tuple format
            results.append((
                alert['id'],
                alert['alert_time'],
                alert['email_id'],
                alert['alert_type'],
                alert['priority'],
                alert['triggering_value'],
                alert['threshold_value'],
                alert['details'],
                alert['acknowledged'],
                alert['acknowledged_at'],
                alert['acknowledged_by']
            ))
        
        return sorted(results, key=lambda x: x[1], reverse=True)  # Sort by alert_time DESC
    
    # Checkpoint Methods
    
    def update_last_synch(self, email_id, timestamp):
        """Update last synchronization timestamp."""
        for email in self._email_addresses:
            if email['id'] == email_id:
                email['last_synch'] = timestamp
                return True
        return False
    
    def get_last_synch(self, email_id):
        """Get last synchronization timestamp."""
        for email in self._email_addresses:
            if email['id'] == email_id:
                return email.get('last_synch')
        return None
    
    def update_daily_summaries_checkpoint(self, email_id, date):
        """Update daily summaries checkpoint."""
        for email in self._email_addresses:
            if email['id'] == email_id:
                email['daily_summaries_checkpoint'] = date
                return True
        return False
    
    def update_intraday_checkpoint(self, email_id, timestamp):
        """Update intraday checkpoint."""
        for email in self._email_addresses:
            if email['id'] == email_id:
                email['intraday_checkpoint'] = timestamp
                return True
        return False
    
    # Utility Methods for Testing
    
    def reset(self):
        """Reset all data (useful for test cleanup)."""
        self._admin_users = []
        self._email_addresses = []
        self._daily_summaries = []
        self._intraday_metrics = []
        self._sleep_logs = []
        self._alerts = []
        self._pending_authorizations = []
        
        self._admin_user_id_counter = 1
        self._email_id_counter = 1
        self._alert_id_counter = 1
        self._pending_auth_id_counter = 1
    
    def get_all_data(self):
        """Get all data for inspection (useful for debugging tests)."""
        return {
            'admin_users': copy.deepcopy(self._admin_users),
            'email_addresses': copy.deepcopy(self._email_addresses),
            'daily_summaries': copy.deepcopy(self._daily_summaries),
            'intraday_metrics': copy.deepcopy(self._intraday_metrics),
            'sleep_logs': copy.deepcopy(self._sleep_logs),
            'alerts': copy.deepcopy(self._alerts)
        }
