"""
Services Package

Services contain business logic that orchestrates multiple repositories
or performs complex calculations on data retrieved from repositories.

Services should:
- Use repositories to fetch/store data
- Implement business rules and calculations
- Coordinate multiple repository operations
- Return processed/transformed data
- NOT contain SQL queries

Example:
    from services import DeviceStatisticsService
    from database import ConnectionManager

    with ConnectionManager() as conn:
        stats_service = DeviceStatisticsService(conn)
        usage = stats_service.get_last_device_usage_statistics(device_id, timedelta(days=7))
"""

from services.care_provider_service import CareProviderService
from services.device_service import DeviceService
from services.device_statistics_service import DeviceStatisticsService
from services.staff_user_service import StaffUserService
from services.collectors import (
    FitbitDailySummaryCollectorService,
    FitbitSleepCollectorService,
    FitbitIntradayCollectorService,
)

__all__ = [
    'CareProviderService',
    'DeviceService',
    'DeviceStatisticsService',
    'StaffUserService',
    'FitbitDailySummaryCollectorService',
    'FitbitSleepCollectorService',
    'FitbitIntradayCollectorService',
]
