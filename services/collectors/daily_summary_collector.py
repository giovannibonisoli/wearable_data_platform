"""
Daily Summary Collector Service.

Collects daily aggregate metrics (steps, heart rate, sleep, nutrition, etc.)
from the Google Health API for authorized devices.
"""

import time
import logging
from datetime import datetime, timedelta

from database import DeviceRepository, MetricsRepository
from database.orm_models import DeviceModel
from database.connection import SqlalchemyConnection
from services.integrations.oauth_provider import create_api_client
from services.collectors.base_collector import BaseCollector
from services.result_enums import CollectorResult

logger = logging.getLogger(__name__)

# First date to collect when no checkpoint exists
DEFAULT_START_DATE = datetime(2025, 1, 21).date()


class DailySummaryCollectorService(BaseCollector):
    """Collects daily summary metrics from the Google Health API."""

    def __init__(self, conn: SqlalchemyConnection):
        super().__init__(conn)
        self.metrics_repo = MetricsRepository(conn)

    def _fetch_and_store_daily_summary(
        self, client, device_id: int, email_address: str, date_obj
    ) -> tuple[bool, bool]:
        """Fetch and store one day's summary. Returns (success, rate_limited)."""
        date_str = date_obj.strftime("%Y-%m-%d")

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
            # Activity / daily aggregates
            summary_data, rate_limited = client._translate_activity_summary(date_str, optional=True)
            if rate_limited:
                return False, True
            if summary_data:
                summary = summary_data.get("summary", {})
                data.update({
                    "steps": summary.get("steps", 0),
                    "distance": summary.get("distances", [{}])[0].get("distance", 0),
                    "calories": summary.get("caloriesOut", 0),
                    "floors": summary.get("floors", 0),
                    "elevation": summary.get("elevation", 0),
                    "active_minutes": summary.get("veryActiveMinutes", 0),
                    "sedentary_minutes": summary.get("sedentaryMinutes", 0),
                })

            # Resting heart rate
            hr_data, rate_limited = client._translate_heart_daily(date_str, optional=True)
            if rate_limited:
                return False, True
            if hr_data:
                data["heart_rate"] = hr_data.get("activities-heart", [{}])[0].get("value", {}).get("restingHeartRate", 0)

            # Sleep minutes
            sleep_data, rate_limited = client._translate_sleep(date_str, optional=True)
            if rate_limited:
                return False, True
            if sleep_data:
                data["sleep_minutes"] = sum(log.get("minutesAsleep", 0) for log in sleep_data.get("sleep", []))

            # Nutrition calories
            food_data, rate_limited = client._translate_food(date_str, optional=True)
            if rate_limited:
                return False, True
            if food_data:
                data["nutrition_calories"] = food_data.get("summary", {}).get("calories", 0)

            # Water
            water_data, rate_limited = client._translate_water(date_str, optional=True)
            if rate_limited:
                return False, True
            if water_data:
                data["water"] = water_data.get("summary", {}).get("water", 0)

            # Oxygen saturation
            spo2_data, rate_limited = client._translate_spo2_daily(date_str, optional=True)
            if rate_limited:
                return False, True
            if spo2_data:
                data["oxygen_saturation"] = float(
                    spo2_data.get("value", {}).get("avg", 0) if isinstance(spo2_data.get("value"), dict) else spo2_data.get("value", 0)
                )

            # Breathing rate
            br_data, rate_limited = client._translate_br_daily(date_str, optional=True)
            if rate_limited:
                return False, True
            if br_data:
                data["respiratory_rate"] = float(
                    br_data.get("value", {}).get("breathingRate", 0) if isinstance(br_data.get("value"), dict) else br_data.get("value", 0)
                )

            # Temperature
            temp_data, rate_limited = client._translate_temperature(date_str, optional=True)
            if rate_limited:
                return False, True
            if temp_data:
                data["temperature"] = temp_data.get("value", 0)

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

        except Exception as e:
            logger.error(f"Unexpected error fetching summary for device {device_id} on {date_str}: {e}")
            return False, False

    def _process_one_device(self, device: DeviceModel) -> str:
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

        client = create_api_client(
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
