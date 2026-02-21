"""
Fitbit Intraday Collector Service.

Collects minute-level intraday metrics (heart rate, steps, calories, distance, etc.)
from Fitbit API for authorized devices. Processes one date per device per cycle.
"""

import time
import logging
import requests
from datetime import datetime, timedelta

from database import ConnectionManager, DeviceRepository, MetricsRepository, Device
from services.integrations.fitbit import fetch_fitbit_endpoint, refresh_tokens, get_device_info
from services.collectors.base_fitbit_collector import BaseFitbitCollector
from services.result_enums import CollectorResult

logger = logging.getLogger(__name__)

# First date to collect when no checkpoint exists
DEFAULT_START_DATE = datetime(2025, 11, 18).date()


class FitbitIntradayCollectorService(BaseFitbitCollector):
    """Collects intraday (minute-level) metrics from Fitbit API."""

    def __init__(self, conn: ConnectionManager):
        super().__init__(conn)
        self.metrics_repo = MetricsRepository(conn)

    def _fetch_and_store_intraday_day(
        self, access_token: str, device: Device, date_str: str, last_synch_date: datetime
    ) -> tuple[bool, bool]:
        """Fetch and store intraday data for one date. Returns (success, rate_limited)."""
        detail_level = "1min"
        metrics_config = [
            ("heart_rate", f"https://api.fitbit.com/1/user/-/activities/heart/date/{date_str}/1d/{detail_level}.json", "activities-heart-intraday"),
            ("steps", f"https://api.fitbit.com/1/user/-/activities/steps/date/{date_str}/1d/{detail_level}.json", "activities-steps-intraday"),
            ("calories", f"https://api.fitbit.com/1/user/-/activities/calories/date/{date_str}/1d/{detail_level}.json", "activities-calories-intraday"),
            ("distance", f"https://api.fitbit.com/1/user/-/activities/distance/date/{date_str}/1d/{detail_level}.json", "activities-distance-intraday"),
            ("floors", f"https://api.fitbit.com/1/user/-/activities/floors/date/{date_str}/1d/{detail_level}.json", "activities-floors-intraday"),
            ("elevation", f"https://api.fitbit.com/1/user/-/activities/elevation/date/{date_str}/1d/{detail_level}.json", "activities-elevation-intraday"),
        ]

        data_points: dict = {}
        for data_type, url, key in metrics_config:
            data, rate_limited = fetch_fitbit_endpoint(url, access_token, optional=False)
            if rate_limited:
                logger.warning(f"Rate limit hit for {device.email_address} on {data_type}")
                return False, True

            if data and key in data:
                dataset = data[key].get("dataset", [])
                for point in dataset:
                    time_str = point.get("time")
                    value = point.get("value")
                    if time_str and value is not None:
                        timestamp = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
                        if last_synch_date.tzinfo:
                            timestamp = timestamp.replace(tzinfo=last_synch_date.tzinfo)
                        if timestamp not in data_points:
                            data_points[timestamp] = {}
                        data_points[timestamp][data_type] = value

        timestamps = [t for t in data_points if t <= last_synch_date]
        timestamps.sort()

        total_points = 0
        last_timestamp = None
        for timestamp in timestamps:
            values = data_points[timestamp]
            steps = values.get("steps", 0)
            distance = values.get("distance", 0)
            heart_rate = values.get("heart_rate")
            is_empty = heart_rate is None and steps == 0 and distance == 0
            if not is_empty:
                self.metrics_repo.insert_intraday_metric(
                    device.id, timestamp, data_type="heart_rate", value=values.get("heart_rate")
                )
                self.metrics_repo.insert_intraday_metric(
                    device.id, timestamp, data_type="steps", value=steps
                )
                self.metrics_repo.insert_intraday_metric(
                    device.id, timestamp, data_type="distance", value=distance
                )
                self.metrics_repo.insert_intraday_metric(
                    device.id, timestamp, data_type="calories", value=values.get("calories")
                )
                self.metrics_repo.insert_intraday_metric(
                    device.id, timestamp, data_type="floors", value=values.get("floors")
                )
                self.metrics_repo.insert_intraday_metric(
                    device.id, timestamp, data_type="elevation", value=values.get("elevation")
                )
                total_points += 1

            last_timestamp = timestamp
            self.device_repo.update_intraday_checkpoint(device.id, timestamp)

        if total_points > 0:
            logger.info(f"Collected {total_points} intraday points for {device.email_address} on {date_str}")
            return True, False
        else:
            logger.warning(f"No intraday data for {device.email_address} on {date_str}")
            return False, False

    def _process_one_device(self, device: Device) -> str:
        device_id = device.id
        email_address = device.email_address

        intraday_checkpoint = device.intraday_checkpoint
        if intraday_checkpoint:
            current_dt = intraday_checkpoint + timedelta(minutes=1)
        else:
            current_dt = datetime.combine(DEFAULT_START_DATE, datetime.min.time())
            self.device_repo.update_intraday_checkpoint(device_id, current_dt)

        access_token, refresh_token = self.device_repo.get_tokens(device_id)
        if not access_token or not refresh_token:
            logger.warning(f"No tokens for device {device_id} ({email_address})")
            return CollectorResult.ERROR.value

        last_synch = device.last_synch
        if not last_synch:
            logger.warning(f"No last_synch for device {device_id}")
            return CollectorResult.ERROR.value

        # Refresh last_synch from API if we're caught up
        if current_dt >= last_synch:
            try:
                device_data = get_device_info(access_token)
                new_last_synch = device_data["lastSyncTime"]
                if last_synch.tzinfo:
                    new_last_synch = new_last_synch.replace(tzinfo=last_synch.tzinfo)
                if new_last_synch != last_synch:
                    self.device_repo.update_last_synch(
                        device_id, new_last_synch.strftime("%Y-%m-%d %H:%M:%S")
                    )
                    last_synch = new_last_synch
                logger.info(f"Device {device_id} ({email_address}) is up to date (last: {last_synch})")
                return CollectorResult.SUCCESS.value
            except Exception as e:
                logger.error(f"Failed to refresh last_synch for {email_address}: {e}")
                return CollectorResult.ERROR.value

        if current_dt >= last_synch:
            return CollectorResult.SUCCESS.value

        date_str = current_dt.strftime("%Y-%m-%d")

        def _try_collect(acc_token: str) -> tuple[bool, bool]:
            return self._fetch_and_store_intraday_day(
                acc_token, device, date_str, last_synch
            )

        try:
            success, rate_limited = _try_collect(access_token)
            if rate_limited:
                return CollectorResult.RATE_LIMITED.value
            return CollectorResult.SUCCESS.value if success else CollectorResult.ERROR.value
        except requests.exceptions.HTTPError as e:
            if hasattr(e, "response") and e.response and e.response.status_code == 401:
                logger.warning(f"Token expired for {email_address}, refreshing...")
                new_access, new_refresh = refresh_tokens(refresh_token)
                if new_access and new_refresh:
                    self.device_repo.update_tokens(device_id, new_access, new_refresh)
                    try:
                        success, rate_limited = _try_collect(new_access)
                        if rate_limited:
                            return CollectorResult.RATE_LIMITED.value
                        return CollectorResult.SUCCESS.value if success else CollectorResult.ERROR.value
                    except Exception as e2:
                        logger.error(f"Error after token refresh for {email_address}: {e2}")
                        return CollectorResult.ERROR.value
                else:
                    logger.error(f"Failed to refresh token for {email_address}")
                    return CollectorResult.ERROR.value
            logger.error(f"HTTP error for {email_address}: {e}")
            return CollectorResult.ERROR.value
        except Exception as e:
            logger.error(f"Unexpected error for {email_address}: {e}", exc_info=True)
            return CollectorResult.ERROR.value
