"""
Fitbit Sleep Collector Service.

Collects detailed sleep sessions (logs, levels, short levels) from Fitbit API
for authorized devices.
"""

import time
import logging
import requests
from datetime import datetime, timedelta

from database import ConnectionManager, DeviceRepository, SleepRepository, Device
from services.integrations.fitbit import fetch_fitbit_endpoint, refresh_tokens
from services.collectors.base_fitbit_collector import BaseFitbitCollector
from services.result_enums import CollectorResult

logger = logging.getLogger(__name__)

# First date to collect when no checkpoint exists
DEFAULT_START_DATE = datetime(2025, 1, 24).date()


class FitbitSleepCollectorService(BaseFitbitCollector):
    """Collects sleep session data from Fitbit API."""

    def __init__(self, conn: ConnectionManager):
        super().__init__(conn)
        self.sleep_repo = SleepRepository(conn)

    def _fetch_and_store_sleep_logs(
        self, access_token: str, device_id: int, date_obj
    ) -> tuple[bool, bool]:
        """Fetch and store sleep logs for one date. Returns (success, rate_limited)."""
        date_str = date_obj.strftime("%Y-%m-%d")
        url = f"https://api.fitbit.com/1.2/user/-/sleep/date/{date_str}.json"

        data, rate_limited = fetch_fitbit_endpoint(url, access_token, optional=False)
        if rate_limited:
            return False, True

        if not data or "sleep" not in data:
            return True, False

        for sleep_log in data["sleep"]:
            sleep_session_id = self.sleep_repo.create_session(device_id)
            if sleep_session_id:
                self.sleep_repo.insert_sleep_log(sleep_session_id, sleep_log)

                for level in sleep_log.get("levels", {}).get("data", []):
                    self.sleep_repo.insert_sleep_level(sleep_session_id, level)

                if sleep_log.get("type") == "stages":
                    for short_data in sleep_log.get("levels", {}).get("shortData", []):
                        self.sleep_repo.insert_sleep_short_level(sleep_session_id, short_data)

        if len(data["sleep"]) == 0:
            logger.info(f"No sleep logs found for device {device_id} on {date_obj}")

        return True, False

    def _process_one_device(self, device: Device) -> str:
        device_id = device.id
        email_address = device.email_address

        logger.info(f"Processing sleep logs for device {device_id} ({email_address})")

        access_token, refresh_token = self.device_repo.get_tokens(device_id)
        if not access_token or not refresh_token:
            logger.warning(f"No tokens for device {device_id} ({email_address})")
            return CollectorResult.ERROR.value

        last_date = device.sleep_checkpoint
        if last_date:
            start_date = last_date + timedelta(days=1)
        else:
            start_date = DEFAULT_START_DATE

        if not device.last_synch:
            logger.warning(f"No last_synch for device {device_id}")
            return CollectorResult.ERROR.value

        end_date = device.last_synch.date() - timedelta(days=1)

        if start_date > end_date:
            logger.info(f"Device {device_id} ({email_address}) is up to date for sleep")
            return CollectorResult.SUCCESS.value

        current_date = start_date

        while current_date <= end_date:
            try:
                success, rate_limited = self._fetch_and_store_sleep_logs(
                    access_token, device_id, current_date
                )

                if rate_limited:
                    logger.info(f"Rate limit reached for device {device_id} on {current_date}")
                    return CollectorResult.RATE_LIMITED.value

                if not success:
                    logger.warning(
                        f"Failed to fetch sleep logs for device {device_id} on {current_date}, continuing..."
                    )
                    current_date += timedelta(days=1)
                    continue

                self.device_repo.update_sleep_checkpoint(device_id, current_date)
                current_date += timedelta(days=1)
                time.sleep(1)

            except requests.exceptions.HTTPError as e:
                if hasattr(e, "response") and e.response and e.response.status_code == 401:
                    logger.warning(f"Token expired for {email_address}, refreshing...")
                    new_access, new_refresh = refresh_tokens(refresh_token)
                    if new_access and new_refresh:
                        self.device_repo.update_tokens(device_id, new_access, new_refresh)
                        access_token = new_access
                        refresh_token = new_refresh
                        logger.info(f"Token refreshed for device {device_id} ({email_address})")
                        continue
                    else:
                        logger.error(f"Failed to refresh token for device {device_id} ({email_address})")
                        return CollectorResult.ERROR.value
                else:
                    logger.error(f"HTTP error for device {device_id} on {current_date}: {e}")
                    return CollectorResult.ERROR.value
            except Exception as e:
                logger.error(f"Unexpected error for device {device_id} on {current_date}: {e}")
                return CollectorResult.ERROR.value

        logger.info(f"Completed sleep for device {device_id} ({email_address}) up to {end_date}")
        return CollectorResult.SUCCESS.value
