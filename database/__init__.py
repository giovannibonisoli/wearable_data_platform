"""
Database package with repository pattern implementation.
"""

from database.connection import ConnectionManager, SqlalchemyConnection
from database.repositories.staff_user_repository import StaffUserRepository
from database.repositories.device_repository import DeviceRepository
from database.repositories.metrics_repository import MetricsRepository
from database.repositories.sleep_repository import SleepRepository
from database.repositories.authorization_repository import AuthorizationRepository
from database.repositories.care_provider_repository import CareProviderRepository

from database.orm_models import (
    UserModel,
    StaffProfileModel,
    CareProviderModel,
    DeviceModel,
    PendingAuthorizationModel,
    DailySummaryModel,
    IntradayMetricModel,
    SleepSessionModel,
    SleepLogModel,
    SleepLevelModel,
    SleepShortLevelModel,
    SpO2IntradayModel,
    HRVIntradayModel,
    BreathingRateIntradayModel
)

__all__ = [
    'ConnectionManager',
    'SqlalchemyConnection',
    'StaffUserRepository',
    'DeviceRepository',
    'MetricsRepository',
    'SleepRepository',
    'AuthorizationRepository',
    'CareProviderRepository',
    'UserModel',
    'StaffProfileModel',
    'CareProviderModel',
    'DeviceModel',
    'PendingAuthorizationModel',
    'DailySummaryModel',
    'IntradayMetricModel',
    'SleepSessionModel',
    'SleepLogModel',
    'SleepLevelModel',
    'SleepShortLevelModel',
    'SpO2IntradayModel',
    'HRVIntradayModel',
    'BreathingRateIntradayModel',
    'AlertModel',
]