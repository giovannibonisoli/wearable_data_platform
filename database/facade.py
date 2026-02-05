"""
Database Facade - Unified interface to all repositories.

This provides a backward-compatible interface that mimics the original
DatabaseManager while using the new repository pattern underneath.

Usage:
    # New way (recommended)
    with ConnectionManager() as db:
        admin_repo = AdminUserRepository(db)
        user = admin_repo.verify_credentials(username, password)
    
    # Old way (backward compatible)
    db = Database()
    if db.connect():
        user = db.verify_admin_user(username, password)
        db.close()
"""

from typing import Optional, List, Dict, Any, Tuple, Union
from datetime import datetime, date

from database.connection import ConnectionManager
from database.repositories.admin_repository import AdminUserRepository
from database.repositories.device_repository import DeviceRepository
from database.repositories.metrics_repository import MetricsRepository
from database.repositories.sleep_repository import SleepRepository
from database.repositories.alert_repository import AlertRepository
from database.repositories.authorization_repository import AuthorizationRepository


class Database:
    """
    Unified database interface providing backward compatibility.
    
    This class delegates to specialized repositories while maintaining
    the same interface as the original DatabaseManager.
    """
    
    def __init__(self):
        """Initialize the database facade with a connection manager."""
        self.db = ConnectionManager()
        
        # Initialize repositories (lazy loaded when needed)
        self._admin_repo = None
        self._device_repo = None
        self._metrics_repo = None
        self._sleep_repo = None
        self._alert_repo = None
        self._auth_repo = None
    
    @property
    def admin(self) -> AdminUserRepository:
        """Get or create admin repository."""
        if self._admin_repo is None:
            self._admin_repo = AdminUserRepository(self.db)
        return self._admin_repo
    
    @property
    def devices(self) -> DeviceRepository:
        """Get or create device repository."""
        if self._device_repo is None:
            self._device_repo = DeviceRepository(self.db)
        return self._device_repo
    
    @property
    def metrics(self) -> MetricsRepository:
        """Get or create metrics repository."""
        if self._metrics_repo is None:
            self._metrics_repo = MetricsRepository(self.db)
        return self._metrics_repo
    
    @property
    def sleep(self) -> SleepRepository:
        """Get or create sleep repository."""
        if self._sleep_repo is None:
            self._sleep_repo = SleepRepository(self.db)
        return self._sleep_repo
    
    @property
    def alerts(self) -> AlertRepository:
        """Get or create alert repository."""
        if self._alert_repo is None:
            self._alert_repo = AlertRepository(self.db)
        return self._alert_repo
    
    @property
    def auth(self) -> AuthorizationRepository:
        """Get or create authorization repository."""
        if self._auth_repo is None:
            self._auth_repo = AuthorizationRepository(self.db)
        return self._auth_repo
    
    # Connection management (delegate to ConnectionManager)
    def connect(self) -> bool:
        """Open database connection."""
        return self.db.connect()
    
    def close(self) -> None:
        """Close database connection."""
        self.db.close()
    
    def commit(self) -> None:
        """Commit current transaction."""
        self.db.commit()
    
    def rollback(self) -> None:
        """Rollback current transaction."""
        self.db.rollback()
    
    # Backward-compatible methods (delegate to appropriate repositories)
    
    # Admin user methods
    def verify_admin_user(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """Authenticate an admin user."""
        user = self.admin.verify_credentials(username, password)
        if user:
            return {
                'id': user.id,
                'username': user.username,
                'full_name': user.full_name
            }
        return None
    
    def get_admin_user_by_id(self, admin_user_id: int) -> Optional[Dict[str, Any]]:
        """Fetch admin user profile."""
        user = self.admin.get_by_id(admin_user_id)
        if user:
            return {
                'username': user.username,
                'full_name': user.full_name,
                'created_at': user.created_at,
                'last_login': user.last_login
            }
        return None
    
    def get_admin_user_devices(self, admin_user_id: int) -> Optional[List[Tuple[Any, ...]]]:
        """List devices for an admin user."""
        devices = self.devices.get_by_admin_user(admin_user_id)
        return [
            (d.id, d.email_address, d.authorization_status, d.device_type)
            for d in devices
        ] if devices else None
    
    def update_admin_user_password(self, admin_user_id: int, new_password: str) -> bool:
        """Change admin user password."""
        return self.admin.update_password(admin_user_id, new_password)
    
    def get_all_admin_users(self) -> Optional[List[Tuple[Any, ...]]]:
        """Retrieve all admin users."""
        users = self.admin.get_all()
        return [
            (u.id, u.username, u.email, u.full_name, u.created_at, u.last_login, u.is_active)
            for u in users
        ] if users else None
    
    # Device methods
    def add_device(
        self, 
        admin_user_id: int, 
        email_address: str,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None
    ) -> Optional[int]:
        """Insert a new device."""
        return self.devices.create(admin_user_id, email_address, access_token, refresh_token)
    
    def change_device_status(self, device_id: int, auth_status: str) -> bool:
        """Update device authorization status."""
        return self.devices.update_status(device_id, auth_status)
    
    def get_device_by_email_address(self, email_address: str) -> Optional[int]:
        """Find device by email."""
        device = self.devices.get_by_email(email_address)
        return device.id if device else None
    
    def get_device_tokens(self, device_id: int) -> Tuple[Optional[str], Optional[str]]:
        """Fetch device tokens."""
        return self.devices.get_tokens(device_id)
    
    def update_device_tokens(self, device_id: int, access_token: str, refresh_token: str) -> bool:
        """Update device tokens."""
        return self.devices.update_tokens(device_id, access_token, refresh_token)
    
    def update_device_type(self, device_id: int, device_type: str) -> bool:
        """Update device type."""
        return self.devices.update_device_type(device_id, device_type)
    
    def update_last_synch(self, device_id: int, timestamp: datetime) -> bool:
        """Update last sync timestamp."""
        return self.devices.update_last_synch(device_id, timestamp)
    
    def update_daily_summaries_checkpoint(self, device_id: int, date_value: date) -> bool:
        """Update daily summaries checkpoint."""
        return self.devices.update_daily_summaries_checkpoint(device_id, date_value)
    
    def update_intraday_checkpoint(self, device_id: int, timestamp: datetime) -> bool:
        """Update intraday checkpoint."""
        return self.devices.update_intraday_checkpoint(device_id, timestamp)
    
    def update_sleep_checkpoint(self, device_id: int, date_value: date) -> bool:
        """Update sleep checkpoint."""
        return self.devices.update_sleep_checkpoint(device_id, date_value)
    
    def get_last_synch(self, device_id: int) -> Optional[datetime]:
        """Get last sync timestamp."""
        return self.devices.get_last_synch(device_id)
    
    def get_daily_summary_checkpoint(self, device_id: int) -> Optional[date]:
        """Get daily summaries checkpoint."""
        return self.devices.get_daily_summary_checkpoint(device_id)
    
    def get_intraday_checkpoint(self, device_id: int) -> Optional[datetime]:
        """Get intraday checkpoint."""
        return self.devices.get_intraday_checkpoint(device_id)
    
    def get_sleep_checkpoint(self, device_id: int) -> Optional[date]:
        """Get sleep checkpoint."""
        return self.devices.get_sleep_checkpoint(device_id)
    
    def get_all_devices(self) -> List[Dict[str, Any]]:
        """Get all authorized devices."""
        return self.devices.get_all_authorized()
    
    # Metrics methods
    def get_daily_summaries(
        self,
        device_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[Tuple[Any, ...]]:
        """Get daily summaries."""
        summaries = self.metrics.get_daily_summaries(device_id, start_date, end_date)
        # Convert to tuples for backward compatibility
        return [
            (s.id, s.device_id, s.date, s.steps, s.heart_rate, s.sleep_minutes,
             s.calories, s.distance, s.floors, s.elevation, s.active_minutes,
             s.sedentary_minutes, s.nutrition_calories, s.water, s.weight,
             s.bmi, s.fat, s.oxygen_saturation, s.respiratory_rate, s.temperature)
            for s in summaries
        ]
    
    def insert_daily_summary(self, device_id: int, date: date, **data) -> bool:
        """Insert daily summary."""
        return self.metrics.insert_daily_summary(device_id, date, **data)
    
    def get_device_history(self, device_id: int) -> List[Tuple[Any, ...]]:
        """Get device history."""
        return self.get_daily_summaries(device_id)
    
    def get_intraday_metrics(
        self,
        device_id: int,
        metric_type: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Optional[List[Tuple[datetime, float]]]:
        """Get intraday metrics."""
        return self.metrics.get_intraday_metrics(device_id, metric_type, start_time, end_time)
    
    def check_intraday_timestamp(self, device_id: int, timestamp: datetime) -> bool:
        """Check if intraday timestamp exists."""
        return self.metrics.check_intraday_timestamp_exists(device_id, timestamp)
    
    def insert_intraday_metric(
        self,
        device_id: int,
        timestamp: datetime,
        data_type: str = "heart_rate",
        value: Optional[float] = None,
    ) -> bool:
        """Insert intraday metric."""
        return self.metrics.insert_intraday_metric(device_id, timestamp, data_type, value)
    
    def get_intraday_data_timestamps_by_range(
        self, 
        device_id: int, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[datetime]:
        """Get intraday timestamps in range."""
        return self.metrics.get_intraday_timestamps_by_range(device_id, start_date, end_date)
    
    # Sleep methods
    def insert_sleep_session(self, device_id: int) -> Optional[int]:
        """Create sleep session."""
        return self.sleep.create_session(device_id)
    
    def insert_sleep_log(self, sleep_session_id: int, data: Dict[str, Any]) -> bool:
        """Insert sleep log."""
        return self.sleep.insert_sleep_log(sleep_session_id, data)
    
    def insert_sleep_level(self, sleep_session_id: int, data: Dict[str, Any]) -> bool:
        """Insert sleep level."""
        return self.sleep.insert_sleep_level(sleep_session_id, data)
    
    def insert_sleep_short_level(self, sleep_session_id: int, short: Dict[str, Any]) -> bool:
        """Insert sleep short level."""
        return self.sleep.insert_sleep_short_level(sleep_session_id, short)
    
    def get_sleep_logs(
        self,
        device_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Optional[List[Tuple[Any, ...]]]:
        """Get sleep logs."""
        logs = self.sleep.get_sleep_logs(device_id, start_date, end_date)
        # Convert to tuples for backward compatibility
        return [
            (log.id, log.sleep_session_id, log.start_time, log.end_time,
             log.is_main_sleep, log.duration, log.minutes_asleep,
             log.minutes_awake, log.minutes_in_the_bed, log.log_type, log.type)
            for log in logs
        ] if logs else None
    
    # Alert methods
    def get_user_alerts(
        self,
        email_id: int,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        acknowledged: Optional[bool] = None,
    ) -> Optional[List[Tuple[Any, ...]]]:
        """Get user alerts."""
        alerts_list = self.alerts.get_alerts(email_id, start_time, end_time, acknowledged)
        # Convert to tuples for backward compatibility
        return [
            (a.id, a.email_id, a.alert_type, a.priority, a.triggering_value,
             a.threshold_value, a.alert_time, a.details, a.acknowledged)
            for a in alerts_list
        ] if alerts_list else None
    
    def insert_alert(
        self,
        email_id: int,
        alert_type: str,
        priority: str,
        triggering_value: float,
        threshold: Union[str, float],
        timestamp: Optional[datetime] = None,
        details: Optional[str] = None
    ) -> Optional[int]:
        """Insert alert."""
        return self.alerts.create(
            email_id, alert_type, priority, triggering_value, 
            threshold, timestamp, details
        )
    
    def get_alert_by_id(self, alert_id: int) -> Optional[Dict[str, Any]]:
        """Get alert by ID."""
        return self.alerts.get_by_id(alert_id)
    
    # Authorization methods
    def store_pending_auth(self, device_id: int, state: str, code_verifier: str) -> bool:
        """Store pending authorization."""
        return self.auth.store_pending_auth(device_id, state, code_verifier)
    
    def get_pending_auth(self, state: str) -> Optional[Dict[str, Any]]:
        """Get pending authorization."""
        return self.auth.get_by_state(state)
    
    def check_pending_auth(self, device_id: int) -> bool:
        """Check if pending auth exists."""
        return self.auth.check_exists(device_id)
    
    def delete_pending_auth(self, state: str) -> bool:
        """Delete pending authorization."""
        return self.auth.delete_by_state(state)
    
    # Context manager support
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if exc_type is not None:
            self.rollback()
        self.close()
