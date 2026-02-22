"""
Fitbit Daily Summary Collector Service.

Collects daily aggregate metrics (steps, heart rate, sleep, nutrition, etc.)
from Fitbit API for authorized devices.
"""

import time
import logging
import requests
from datetime import datetime, timedelta

from database import ConnectionManager, DeviceRepository, MetricsRepository, Device
from services.integrations.fitbit import FitbitClient
from services.collectors.base_fitbit_collector import BaseFitbitCollector
from services.result_enums import CollectorResult

logger = logging.getLogger(__name__)

# First date to collect when no checkpoint exists
DEFAULT_START_DATE = datetime(2025, 1, 21).date()


class FitbitDailySummaryCollectorService(BaseFitbitCollector):
    """Collects daily summary metrics from Fitbit API."""

    def __init__(self, conn: ConnectionManager):
        super().__init__(conn)
        self.metrics_repo = MetricsRepository(conn)

    def _fetch_and_store_daily_summary(
        self, client: FitbitClient, device_id: int, email_address: str, date_obj
    ) -> tuple[bool, bool]:
        """Fetch and store one day's summary. Returns (success, rate_limited)."""
        date_str = date_obj.strftime("%Y-%m-%d")

        endpoints = [
            (
                f"https://api.fitbit.com/1/user/-/activities/date/{date_str}.json",
                False,
                lambda d: {
                    "steps": d.get("summary", {}).get("steps", 0),
                    "distance": d.get("summary", {}).get("distances", [{}])[0].get("distance", 0),
                    "calories": d.get("summary", {}).get("caloriesOut", 0),
                    "floors": d.get("summary", {}).get("floors", 0),
                    "elevation": d.get("summary", {}).get("elevation", 0),
                    "active_minutes": d.get("summary", {}).get("veryActiveMinutes", 0),
                    "sedentary_minutes": d.get("summary", {}).get("sedentaryMinutes", 0),
                },
            ),
            (
                f"https://api.fitbit.com/1/user/-/activities/heart/date/{date_str}/1d.json",
                False,
                lambda d: {
                    "heart_rate": d.get("activities-heart", [{}])[0].get("value", {}).get("restingHeartRate", 0)
                },
            ),
            (
                f"https://api.fitbit.com/1.2/user/-/sleep/date/{date_str}.json",
                False,
                lambda d: {
                    "sleep_minutes": sum(log.get("minutesAsleep", 0) for log in d.get("sleep", [])),
                },
            ),
            (
                f"https://api.fitbit.com/1/user/-/foods/log/date/{date_str}.json",
                False,
                lambda d: {"nutrition_calories": d.get("summary", {}).get("calories", 0)},
            ),
            (
                f"https://api.fitbit.com/1/user/-/foods/log/water/date/{date_str}.json",
                False,
                lambda d: {"water": d.get("summary", {}).get("water", 0)},
            ),
            (
                f"https://api.fitbit.com/1/user/-/spo2/date/{date_str}.json",
                True,
                lambda d: {
                    "oxygen_saturation": float(
                        d.get("value", {}).get("avg", 0)
                        if isinstance(d.get("value"), dict)
                        else d.get("value", 0)
                    )
                },
            ),
            (
                f"https://api.fitbit.com/1/user/-/br/date/{date_str}.json",
                True,
                lambda d: {
                    "respiratory_rate": float(
                        d.get("value", {}).get("breathingRate", 0)
                        if isinstance(d.get("value"), dict)
                        else d.get("value", 0)
                    )
                },
            ),
            (
                f"https://api.fitbit.com/1/user/-/temp/core/date/{date_str}.json",
                True,
                lambda d: {"temperature": d.get("value", 0)},
            ),
        ]

        data = {
            "steps": 0,
            "distance": 0,
            "calories": 0,
            "floors": 0,
            "elevation": 0,
            "active_minutes": 0,
            "sedentary_minutes": 0,
            "heart_rate": 0,
            "sleep_minutes": 0,
            "nutrition_calories": 0,
            "water": 0,
            "oxygen_saturation": 0,
            "respiratory_rate": 0,
            "temperature": 0,
        }

        try:
            for url, optional, extractor in endpoints:
                response_data, rate_limited = client.get(url, optional=optional)
                if rate_limited:
                    return False, True
                if response_data:
                    data.update(extractor(response_data))

            # Skip empty/invalid days
            if (
                data["steps"] == 0
                and data["heart_rate"] == 0
                and data["distance"] == 0
                and data["sedentary_minutes"] == 1440
            ):
                return True, False

            self.metrics_repo.insert_daily_summary(
                device_id=device_id, date_value=date_str, **data
            )
            logger.info(f"Daily summary collected for device {device_id} ({email_address}) on {date_str}")
            return True, False

        except requests.exceptions.HTTPError as e:
            if hasattr(e, "response") and e.response and e.response.status_code == 429:
                return False, True
            logger.error(f"HTTP error fetching summary for device {device_id} on {date_str}: {e}")
            return False, False
        except Exception as e:
            logger.error(f"Unexpected error fetching summary for device {device_id} on {date_str}: {e}")
            return False, False

    def _process_one_device(self, device: Device) -> str:
        device_id = device.id
        email_address = device.email_address

        logger.info(f"Processing daily summary for device {device_id} ({email_address})")

        access_token, refresh_token = self.device_repo.get_tokens(device_id)
        if not access_token or not refresh_token:
            logger.warning(f"No tokens for device {device_id} ({email_address})")
            return CollectorResult.ERROR.value

        last_date = device.daily_summaries_checkpoint
        if last_date:
            start_date = last_date + timedelta(days=1)
        else:
            start_date = DEFAULT_START_DATE

        if not device.last_synch:
            logger.warning(f"No last_synch for device {device_id}")
            return CollectorResult.ERROR.value

        end_date = device.last_synch.date() - timedelta(days=1)

        if start_date > end_date:
            logger.info(f"Device {device_id} ({email_address}) is up to date for summaries")
            return CollectorResult.SUCCESS.value

        # One client per device: auto-refreshes and persists tokens on 401
        client = FitbitClient(
            access_token=access_token,
            refresh_token=refresh_token,
            on_tokens_updated=lambda a, r: self.device_repo.update_tokens(device_id, a, r),
        )

        current_date = start_date

        while current_date <= end_date:
            try:
                success, rate_limited = self._fetch_and_store_daily_summary(
                    client, device_id, email_address, current_date
                )

                if rate_limited:
                    logger.info(f"Rate limit reached for device {device_id} on {current_date}")
                    return CollectorResult.RATE_LIMITED.value

                if not success:
                    logger.warning(
                        f"Failed to fetch summary for device {device_id} on {current_date}, continuing..."
                    )
                    current_date += timedelta(days=1)
                    continue

                self.device_repo.update_daily_summaries_checkpoint(device_id, current_date)
                current_date += timedelta(days=1)
                time.sleep(1)

            except Exception as e:
                logger.error(f"Unexpected error for device {device_id} on {current_date}: {e}")
                return CollectorResult.ERROR.value

        logger.info(f"Completed summaries for device {device_id} ({email_address}) up to {end_date}")
        return CollectorResult.SUCCESS.value
