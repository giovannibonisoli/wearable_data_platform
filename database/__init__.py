"""
Database package with repository pattern implementation.

This package provides a clean separation of concerns for database operations,
organized into domain-specific repositories.

Quick Start:
-----------

# Option 1: Use the facade for backward compatibility
from database import Database

db = Database()
if db.connect():
    user = db.verify_admin_user(username, password)
    devices = db.get_all_devices()
    db.close()

# Option 2: Use repositories directly (recommended for new code)
from database import ConnectionManager, AdminUserRepository, DeviceRepository

with ConnectionManager() as db:
    admin_repo = AdminUserRepository(db)
    device_repo = DeviceRepository(db)
    
    user = admin_repo.verify_credentials(username, password)
    devices = device_repo.get_all_authorized()

Repository Classes:
------------------
- AdminUserRepository: Admin user authentication and management
- DeviceRepository: Device management, tokens, and sync checkpoints
- MetricsRepository: Daily summaries and intraday health metrics
- SleepRepository: Sleep sessions, logs, and levels
- AlertRepository: Health alerts and notifications
- AuthorizationRepository: OAuth pending authorizations

Models:
-------
- AdminUser, Device, DailySummary, IntradayMetric
- SleepSession, SleepLog, SleepLevel
- Alert, PendingAuthorization
"""

from database.connection import ConnectionManager
from database.facade import Database

# Import repositories for direct use
from database.repositories.admin_repository import AdminUserRepository
from database.repositories.device_repository import DeviceRepository
from database.repositories.metrics_repository import MetricsRepository
from database.repositories.sleep_repository import SleepRepository
from database.repositories.alert_repository import AlertRepository
from database.repositories.authorization_repository import AuthorizationRepository

# Import models
from database.models import (
    AdminUser,
    Device,
    DailySummary,
    IntradayMetric,
    SleepSession,
    SleepLog,
    SleepLevel,
    Alert,
    PendingAuthorization
)

__all__ = [
    # Core classes
    'ConnectionManager',
    'Database',
    
    # Repositories
    'AdminUserRepository',
    'DeviceRepository',
    'MetricsRepository',
    'SleepRepository',
    'AlertRepository',
    'AuthorizationRepository',
    
    # Models
    'AdminUser',
    'Device',
    'DailySummary',
    'IntradayMetric',
    'SleepSession',
    'SleepLog',
    'SleepLevel',
    'Alert',
    'PendingAuthorization',
]

__version__ = '2.0.0'
