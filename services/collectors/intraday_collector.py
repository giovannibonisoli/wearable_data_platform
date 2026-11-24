"""
Intraday Collector Service.

Collects minute-level intraday metrics (heart rate, steps, calories, distance, etc.)
from the Google Health API for authorized devices. Processes one date per device per cycle.
"""

import logging
from datetime import datetime, timedelta, timezone

from database import DeviceRepository, MetricsRepository
from database.orm_models import DeviceModel
from database.connection import SqlalchemyConnection
from services.integrations.oauth_provider import create_api_client
from services.collectors.base_collector import BaseCollector
from services.result_enums import CollectorResult
from config import COLLECTION_START_DATE

logger = logging.getLogger(__name__)


class IntradayCollectorService(BaseCollector):
    """Collects intraday (minute-level) metrics from the Google Health API."""

    def __init__(self, conn: SqlalchemyConnection):
        super().__init__(conn)
        self.metrics_repo = MetricsRepository(conn)

    def _fetch_and_store_intraday_day(
        self,
        client,
        device: DeviceModel,
        date_str: str,
        window_start: datetime,
        last_synch_date: datetime,
    ) -> tuple[bool, bool]:
        """Fetch and store intraday data for one date. Returns (success, rate_limited)."""
        metrics_config = [
            # (internal data type, Google Health dataType, value field, scale, kind)
            # scale: API unit -> stored unit (distance/elevation: mm -> m)
            # kind: "sample"    -> filtered on the sample's civil time
            #       "interval"  -> filtered on the interval's civil start time
            #       "reconcile" -> reconciled endpoint (no `list` action), interval type
            ("heart_rate", "heart-rate", "beatsPerMinute", 1, "sample"),
            ("steps", "steps", "count", 1, "interval"),
            ("calories", "active-energy-burned", "kcal", 1, "interval"),
            ("distance", "distance", "millimeters", 0.001, "interval"),
            ("floors", "floors", "count", 1, "reconcile"),
            ("elevation", "altitude", "gainMillimeters", 0.001, "interval"),
        ]

        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()

        data_points: dict = {}
        for data_type, gtype, value_field, scale, kind in metrics_config:
            resp, rate_limited = client._list_data_points(
                gtype,
                date_str,
                optional=False,
                sample=(kind == "sample"),
                action="reconcile" if kind == "reconcile" else "list",
            )

            if rate_limited:
                logger.warning(f"Rate limit hit for {device.email_address} on {data_type}")
                return False, True

            dataset = client._to_intraday_dataset(resp, gtype, value_field, scale)
            for point in dataset:
                timestamp = point.get("datetime")
                value = point.get("value")
                if timestamp is None or value is None:
                    continue
                # Defensive: never store a point under a day other than its own.
                if timestamp.date() != target_date:
                    continue
                if last_synch_date.tzinfo:
                    timestamp = timestamp.replace(tzinfo=last_synch_date.tzinfo)
                if timestamp not in data_points:
                    data_points[timestamp] = {}
                data_points[timestamp][data_type] = value

        day_end = datetime.strptime(f"{date_str} 23:59:00", "%Y-%m-%d %H:%M:%S")
        if last_synch_date.tzinfo:
            day_end = day_end.replace(tzinfo=last_synch_date.tzinfo)
        window_end = min(day_end, last_synch_date)

        # Only process points that are strictly after current checkpoint window start.
        timestamps = [t for t in data_points if window_start <= t <= window_end]
        timestamps.sort()

        total_points = 0
        for timestamp in timestamps:
            values = data_points[timestamp]
            steps = values.get("steps", 0)
            distance = values.get("distance", 0)
            heart_rate = values.get("heart_rate")
            is_empty = heart_rate is None and steps == 0 and distance == 0
            if not is_empty:
                self.metrics_repo.insert_intraday_metric(
                    device.id, timestamp, data_type="heart_rate", value=heart_rate
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

            self.device_repo.update_intraday_checkpoint(device.id, timestamp)

        if total_points > 0:
            logger.info(f"Collected {total_points} intraday points for {device.email_address} on {date_str}")
            return True, False
        else:
            # Avoid getting stuck on the same day when the provider has no points in the
            # remaining time window (e.g., last value at 22:59). Move checkpoint to
            # the end of the current collection window so next cycle can advance.
            self.device_repo.update_intraday_checkpoint(device.id, window_end)
            logger.info(
                f"No intraday points in remaining window for {device.email_address} on {date_str}; "
                f"advanced checkpoint to {window_end}"
            )
            return True, False

    def _process_one_device(self, device: DeviceModel) -> str:
        device_id = device.id
        email_address = device.email_address

        last_synch = device.last_synch
        if not last_synch:
            logger.warning(f"No last_synch for device {device_id}")
            return CollectorResult.ERROR.value

        intraday_checkpoint = device.intraday_checkpoint
        if intraday_checkpoint:
            if last_synch.tzinfo:
                if intraday_checkpoint.tzinfo:
                    intraday_checkpoint = intraday_checkpoint.astimezone(last_synch.tzinfo)
                else:
                    intraday_checkpoint = intraday_checkpoint.replace(tzinfo=last_synch.tzinfo)
            current_dt = intraday_checkpoint + timedelta(minutes=1)
            current_dt = current_dt.replace(second=0, microsecond=0)
        else:
            current_dt = datetime.combine(COLLECTION_START_DATE, datetime.min.time())
            if last_synch.tzinfo:
                current_dt = current_dt.replace(tzinfo=last_synch.tzinfo)
            self.device_repo.update_intraday_checkpoint(device_id, current_dt)

        access_token, refresh_token = self.device_repo.get_tokens(device_id)
        if not access_token or not refresh_token:
            logger.warning(f"No tokens for device {device_id} ({email_address})")
            return CollectorResult.ERROR.value

        client = create_api_client(
            access_token=access_token,
            refresh_token=refresh_token,
            on_tokens_updated=lambda a, r: self.device_repo.update_tokens(device_id, a, r),
            on_tokens_revoked=lambda: self.device_repo.mark_reauthorization_required(device_id),
        )

        # Refresh last_synch from API if we're caught up
        if current_dt >= last_synch:
            try:
                device_data = client.get_device_info()
                new_last_synch = device_data["lastSyncTime"]
                if last_synch.tzinfo is None:
                    if new_last_synch.tzinfo is not None:
                        new_last_synch = new_last_synch.astimezone(timezone.utc).replace(tzinfo=None)
                elif new_last_synch.tzinfo is None:
                    new_last_synch = new_last_synch.replace(tzinfo=last_synch.tzinfo)
                else:
                    new_last_synch = new_last_synch.astimezone(last_synch.tzinfo)
                # Never move last_synch backwards if provider returns stale metadata.
                if new_last_synch > last_synch:
                    self.device_repo.update_last_synch(
                        device_id, new_last_synch.strftime("%Y-%m-%d %H:%M:%S")
                    )
                    last_synch = new_last_synch
                logger.info(f"Device {device_id} ({email_address}) is up to date (last: {last_synch})")
                return CollectorResult.UP_TO_DATE.value
            except Exception as e:
                logger.error(f"Failed to refresh last_synch for {email_address}: {e}")
                return CollectorResult.ERROR.value

        if current_dt >= last_synch:
            return CollectorResult.UP_TO_DATE.value

        date_str = current_dt.strftime("%Y-%m-%d")

        try:
            success, rate_limited = self._fetch_and_store_intraday_day(
                client, device, date_str, current_dt, last_synch
            )
            if rate_limited:
                return CollectorResult.RATE_LIMITED.value
            return CollectorResult.SUCCESS.value if success else CollectorResult.ERROR.value
        except Exception as e:
            logger.error(f"Unexpected error for {email_address}: {e}", exc_info=True)
            return CollectorResult.ERROR.value
