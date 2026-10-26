"""
Sleep Collector Service.

Collects detailed sleep sessions (logs, levels, short levels) from the Google Health API
for authorized devices. Also collects physiological metrics tied to sleep:
SpO2 intraday, HRV intraday, and breathing rate intraday — each data point
is matched to the correct sleep session by timestamp overlap.
"""

import time
import logging
from datetime import datetime, timedelta

from database import SleepRepository
from database.orm_models import DeviceModel
from database.connection import SqlalchemyConnection
from services.integrations.oauth_provider import create_api_client
from services.collectors.base_collector import BaseCollector
from services.result_enums import CollectorResult

logger = logging.getLogger(__name__)

# First date to collect when no checkpoint exists
DEFAULT_START_DATE = datetime(2025, 1, 24).date()


class SleepCollectorService(BaseCollector):
    """Collects sleep session data from the Google Health API.

    For each date, collects:
    - Sleep logs, levels, and short levels (one SleepSession per log).
    - SpO2 intraday, HRV intraday, and breathing rate intraday — each point
      is assigned to the sleep session whose window contains its timestamp.
    """

    def __init__(self, conn: SqlalchemyConnection):
        super().__init__(conn)
        self.sleep_repo = SleepRepository(conn)

    # ------------------------------------------------------------------
    # Sleep logs
    # ------------------------------------------------------------------

    def _fetch_and_store_sleep_logs(
        self, client, device_id: int, date_obj
    ) -> tuple[bool, bool, list[dict]]:
        """Fetch and store sleep logs for one date.

        Returns:
            (success, rate_limited, sessions_for_date)

        sessions_for_date is a list of dicts used downstream to assign
        physiological metric points to the correct sleep session:
            {
                "sleep_session_id": int,
                "start_time": datetime,
                "end_time": datetime,
                "is_main_sleep": bool,
            }
        """
        date_str = date_obj.strftime("%Y-%m-%d")

        data, rate_limited = client._translate_sleep(date_str, optional=False)
        if rate_limited:
            return False, True, []

        if not data or "sleep" not in data:
            return True, False, []

        sessions_for_date = []

        for sleep_log in data["sleep"]:
            sleep_session_id = self.sleep_repo.create_session(device_id)
            if not sleep_session_id:
                continue

            self.sleep_repo.insert_sleep_log(sleep_session_id, sleep_log)

            for level in sleep_log.get("levels", {}).get("data", []):
                self.sleep_repo.insert_sleep_level(sleep_session_id, level)

            if sleep_log.get("type") == "stages":
                for short_data in sleep_log.get("levels", {}).get("shortData", []):
                    self.sleep_repo.insert_sleep_short_level(sleep_session_id, short_data)

            # Collect session window metadata for downstream timestamp matching.
            try:
                start_time = datetime.fromisoformat(sleep_log["startTime"])
                end_time = datetime.fromisoformat(sleep_log["endTime"])
            except (KeyError, ValueError) as e:
                logger.warning(
                    f"Could not parse sleep log timestamps for session {sleep_session_id}: {e}"
                )
                continue

            sessions_for_date.append(
                {
                    "sleep_session_id": sleep_session_id,
                    "start_time": start_time,
                    "end_time": end_time,
                    "is_main_sleep": sleep_log.get("isMainSleep", False),
                }
            )

        if not data["sleep"]:
            logger.info(f"No sleep logs found for device {device_id} on {date_obj}")

        return True, False, sessions_for_date

    # ------------------------------------------------------------------
    # Physiological metrics (SpO2 / HRV / Breathing Rate)
    # ------------------------------------------------------------------

    def _match_session_for_timestamp(
        self, timestamp: datetime, sessions_for_date: list[dict]
    ) -> int | None:
        """Return the sleep_session_id whose log window contains this timestamp.

        Falls back to the main sleep session when no exact overlap is found
        (e.g. a metric point recorded just outside the logged sleep window).
        Returns None if sessions_for_date is empty.
        """
        for s in sessions_for_date:
            if s["start_time"] <= timestamp <= s["end_time"]:
                return s["sleep_session_id"]
        # Fallback: main sleep session
        for s in sessions_for_date:
            if s["is_main_sleep"]:
                return s["sleep_session_id"]
        return None

    def _fetch_and_store_sleep_physio(
        self,
        client,
        device_id: int,
        date_obj,
        sessions_for_date: list[dict],
    ) -> tuple[bool, bool]:
        """Fetch SpO2, HRV, and breathing rate intraday for a date.

        Each data point is matched to the correct SleepSession by timestamp
        overlap using _match_session_for_timestamp(). Points that cannot be
        matched are discarded with a warning.

        Returns:
            (success, rate_limited)

        Failures on individual endpoints are non-fatal (optional=True):
        not all devices support all three metrics.
        """
        if not sessions_for_date:
            return True, False

        date_str = date_obj.strftime("%Y-%m-%d")

        # SpO2 intraday
        data, rate_limited = client._translate_spo2_intraday(date_str, optional=True)
        if rate_limited:
            return False, True
        if data:
            matched = 0
            skipped = 0
            for point in data.get("minutes", []):
                raw_ts = point.get("minute")
                if not raw_ts:
                    skipped += 1
                    continue
                try:
                    ts = datetime.fromisoformat(raw_ts)
                except ValueError:
                    skipped += 1
                    continue

                session_id = self._match_session_for_timestamp(ts, sessions_for_date)
                if session_id is None:
                    skipped += 1
                    continue

                self.sleep_repo.insert_spo2(session_id, point)
                matched += 1

            logger.debug(
                f"[spo2] device {device_id} on {date_str}: "
                f"{matched} matched, {skipped} skipped"
            )

        # HRV intraday
        data, rate_limited = client._translate_hrv_intraday(date_str, optional=True)
        if rate_limited:
            return False, True
        if data:
            matched = 0
            skipped = 0
            for point in data.get("minutes", []):
                raw_ts = point.get("minute")
                if not raw_ts:
                    skipped += 1
                    continue
                try:
                    ts = datetime.fromisoformat(raw_ts)
                except ValueError:
                    skipped += 1
                    continue

                session_id = self._match_session_for_timestamp(ts, sessions_for_date)
                if session_id is None:
                    skipped += 1
                    continue

                self.sleep_repo.insert_hrv(session_id, point)
                matched += 1

            logger.debug(
                f"[hrv] device {device_id} on {date_str}: "
                f"{matched} matched, {skipped} skipped"
            )

        # Breathing rate intraday — no data source configured yet (client translator is a stub).

        return True, False

    # ------------------------------------------------------------------
    # Device processing
    # ------------------------------------------------------------------

    def _process_one_device(self, device: DeviceModel) -> str:
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

        client = create_api_client(
            access_token=access_token,
            refresh_token=refresh_token,
            on_tokens_updated=lambda a, r: self.device_repo.update_tokens(device_id, a, r),
        )

        current_date = start_date

        while current_date <= end_date:
            try:
                # --- Sleep logs ---
                success, rate_limited, sessions_for_date = self._fetch_and_store_sleep_logs(
                    client, device_id, current_date
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

                # --- Physiological metrics (non-fatal) ---
                physio_success, physio_rate_limited = self._fetch_and_store_sleep_physio(
                    client, device_id, current_date, sessions_for_date
                )

                if physio_rate_limited:
                    logger.info(
                        f"Rate limit on physio metrics for device {device_id} on {current_date}"
                    )
                    return CollectorResult.RATE_LIMITED.value

                if not physio_success:
                    logger.warning(
                        f"Physio metrics incomplete for device {device_id} on {current_date}, continuing..."
                    )

                self.device_repo.update_sleep_checkpoint(device_id, current_date)
                current_date += timedelta(days=1)
                time.sleep(1)

            except Exception as e:
                logger.error(
                    f"Unexpected error for device {device_id} on {current_date}: {e}",
                    exc_info=True,
                )
                return CollectorResult.ERROR.value

        logger.info(
            f"Completed sleep for device {device_id} ({email_address}) up to {end_date}"
        )
        return CollectorResult.SUCCESS.value
