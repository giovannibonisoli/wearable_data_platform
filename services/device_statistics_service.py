"""
Device Statistics Service

This service layer handles business logic for device usage statistics.
It uses repositories to fetch data and performs calculations/transformations.

Services should:
- Use repositories to fetch data
- Perform business logic and calculations
- Return processed results to controllers/routes
- NOT contain SQL queries
"""

import logging
from datetime import date, datetime, timedelta, timezone
from collections import defaultdict
from typing import Dict, List, Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from config import DISPLAY_TIMEZONE
from database import DeviceRepository, MetricsRepository, SleepRepository
from database.connection import SqlalchemyConnection

logger = logging.getLogger(__name__)

try:
    _DISPLAY_TZ = ZoneInfo(DISPLAY_TIMEZONE)
except (ZoneInfoNotFoundError, ValueError):
    logger.warning(f"Unknown DISPLAY_TIMEZONE '{DISPLAY_TIMEZONE}', showing times in UTC")
    _DISPLAY_TZ = timezone.utc

# Hypnogram rows, top to bottom (lightest to deepest sleep).
# "stages" logs use wake/rem/light/deep, "classic" logs use wake/restless/asleep.
SLEEP_STAGE_ORDER = ("wake", "restless", "rem", "light", "asleep", "deep")
SLEEP_DEFAULT_STAGES = {
    "stages": ("wake", "rem", "light", "deep"),
    "classic": ("wake", "restless", "asleep"),
}


# Vital signs section: selectable metrics
VITAL_METRICS = ("heart_rate", "steps", "calories", "spo2")


class DeviceStatisticsService:
    """
    Service for calculating device usage statistics.
    
    This service encapsulates business logic for analyzing device usage
    patterns, sync status, and data gaps.
    """
    
    def __init__(self, connection_manager: SqlalchemyConnection):
        """
        Initialize the service with a connection manager.
        
        Args:
            connection_manager: Active SqlalchemyConnection instance
        """
        self.conn = connection_manager
        self.device_repo = DeviceRepository(connection_manager)
        self.metrics_repo = MetricsRepository(connection_manager)
        self.sleep_repo = SleepRepository(connection_manager)
    
    def calculate_usage_statistics(
        self, 
        timestamps: List[datetime], 
        max_gap_minutes: int = 5
    ) -> Dict[str, Any]:
        """
        Calculate usage statistics from wearable device timestamps.
        
        This method analyzes a series of timestamps to determine how long
        a device was actively worn/used. Gaps larger than max_gap_minutes
        are considered periods where the device was not worn.
        
        Args:
            timestamps: List of datetime objects representing data points
            max_gap_minutes: Maximum gap between timestamps to consider continuous usage
            
        Returns:
            Dictionary containing:
                - 'total_hours': Total usage hours across all days
                - 'average_hours_per_day': Average usage hours per day
                - 'num_days': Number of days with recorded usage
                - 'hours_per_day': Dict mapping dates to hours (optional)
        """
        if not timestamps or len(timestamps) < 2:
            return {
                'hours_per_day': {},
                'total_hours': 0.0,
                'average_hours_per_day': 0.0,
                'num_days': 0
            }
        
        # Sort timestamps to ensure correct order
        timestamps = sorted(timestamps)
        
        # Dictionary to store total seconds per day
        daily_usage = defaultdict(float)
        
        for i in range(1, len(timestamps)):
            prev_time = timestamps[i-1]
            curr_time = timestamps[i]
            
            gap = curr_time - prev_time
            
            # Only count gaps within the threshold
            if gap <= timedelta(minutes=max_gap_minutes):
                gap_seconds = gap.total_seconds()
                
                # Check if the interval spans multiple days
                if prev_time.date() == curr_time.date():
                    # Same day - add all time to that day
                    daily_usage[prev_time.date()] += gap_seconds
                else:
                    # Different days - split the time
                    # Time until midnight on the first day
                    end_of_prev_day = datetime.combine(
                        prev_time.date(), 
                        datetime.max.time()
                    ).replace(tzinfo=prev_time.tzinfo)
                    
                    time_on_prev_day = (end_of_prev_day - prev_time).total_seconds()
                    daily_usage[prev_time.date()] += time_on_prev_day
                    
                    # Time from midnight on the next day
                    time_on_curr_day = gap_seconds - time_on_prev_day
                    daily_usage[curr_time.date()] += time_on_curr_day
        
        # Convert seconds to hours
        hours_per_day = {date: seconds / 3600 for date, seconds in daily_usage.items()}
        hours_per_day = dict(sorted(hours_per_day.items()))
        
        # Calculate total and average
        total_hours = sum(hours_per_day.values())
        num_days = len(hours_per_day)
        average_hours = total_hours / num_days if num_days > 0 else 0.0
        
        return {
            'hours_per_day': hours_per_day,
            'total_hours': total_hours,
            'average_hours_per_day': average_hours,
            'num_days': num_days
        }
    
    def get_last_device_usage_statistics(
        self, 
        device_id: int, 
        temporal_range: timedelta
    ) -> Dict[str, float]:
        """
        Get device usage statistics for a recent time period.
        
        Calculates how much the device was actually worn/used during
        the specified time range by analyzing intraday data timestamps.
        
        Args:
            device_id: The device identifier
            temporal_range: How far back to look (e.g., timedelta(days=7))
            
        Returns:
            Dictionary with 'total_hours', 'average_hours_per_day', 'num_days'
        """
        # Get last sync time from repository
        last_sync = self.device_repo.get_last_synch(device_id)
        
        if not last_sync:
            return {
                'total_hours': 0,
                'average_hours_per_day': 0,
                'num_days': 0
            }
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - temporal_range
        start_date = start_date.replace(tzinfo=last_sync.tzinfo)
        
        # Only calculate if we have recent data
        if last_sync <= start_date:
            return {
                'total_hours': 0,
                'average_hours_per_day': 0,
                'num_days': 0
            }
        
        # Get timestamps from metrics repository
        timestamps = self.metrics_repo.get_intraday_timestamps_by_range(
            device_id, 
            start_date, 
            end_date
        )
        
        if not timestamps:
            return {
                'total_hours': 0,
                'average_hours_per_day': 0,
                'num_days': 0
            }
        
        # Calculate usage statistics
        usage_stats = self.calculate_usage_statistics(timestamps)
        
        # Return without hours_per_day for cleaner response
        return {
            'total_hours': usage_stats['total_hours'],
            'average_hours_per_day': usage_stats['average_hours_per_day'],
            'num_days': usage_stats['num_days']
        }
    
    def get_hourly_coverage(
        self,
        device_id: int,
        num_days: int,
        now: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Count, for each hour of the last `num_days` days (today included),
        how many distinct minutes of intraday data were received.

        Intraday timestamps hold the wearer's civil (wall-clock) time, so they
        are bucketed by their wall-clock hour without any timezone conversion.

        Returns one row per day, oldest first:
            {'date': date, 'hours': [minutes 0-60, or None for future hours] * 24,
             'total_minutes': int}
        """
        now = now or datetime.now()
        today = now.date()
        first_day = today - timedelta(days=num_days - 1)

        start = datetime.combine(first_day, datetime.min.time())
        end = datetime.combine(today + timedelta(days=1), datetime.min.time())

        # Query bounds are exclusive: widen by one second so 00:00 of the first day is included
        timestamps = self.metrics_repo.get_intraday_timestamps_by_range(
            device_id, start - timedelta(seconds=1), end
        )

        minutes_by_hour = defaultdict(set)
        for ts in timestamps:
            ts = ts.replace(tzinfo=None)
            minutes_by_hour[(ts.date(), ts.hour)].add(ts.minute)

        rows = []
        for offset in range(num_days):
            day = first_day + timedelta(days=offset)
            hours = []
            for hour in range(24):
                if day == today and hour > now.hour:
                    hours.append(None)
                else:
                    hours.append(len(minutes_by_hour.get((day, hour), ())))
            rows.append({
                'date': day,
                'hours': hours,
                'total_minutes': sum(h for h in hours if h),
            })

        return rows

    def get_device_sync_data(self, device_id: int) -> tuple:
        """
        Get device synchronization status and data gap information.
        
        Analyzes when the device was last synced and identifies any gaps
        between the last sync and the last received data.
        
        Args:
            device_id: The device identifier
            
        Returns:
            Tuple of (status, details) where:
                - status: 'ok', 'sync_warning', 'gap_warning', or 'no_data'
                - details: Dict with sync_days, sync_hours, sync_minutes, gap_days
        """
        data_reception_details = {}
        data_reception_status = 'no_data'
        
        # Get sync information from repository
        last_sync = self.device_repo.get_last_synch(device_id)
        
        if not last_sync:
            return data_reception_status, data_reception_details
        
        now = datetime.now()
        last_sync = last_sync.replace(tzinfo=now.tzinfo)
        
        # Calculate time since last sync
        time_diff = now - last_sync
        data_reception_details['sync_days'] = time_diff.days
        data_reception_details['sync_hours'] = time_diff.seconds // 3600
        data_reception_details['sync_minutes'] = time_diff.seconds // 60
        
        # Check for data gap
        intraday_checkpoint = self.device_repo.get_intraday_checkpoint(device_id)
        
        if intraday_checkpoint:
            intraday_checkpoint = intraday_checkpoint.replace(tzinfo=last_sync.tzinfo)
            gap = last_sync - intraday_checkpoint
            data_reception_details['gap_days'] = max(gap.days, 0)
        else:
            data_reception_details['gap_days'] = 0
        
        # Determine overall status
        if data_reception_details['sync_days'] > 7:
            data_reception_status = 'sync_warning'
        elif data_reception_details['gap_days'] > 3:
            data_reception_status = 'gap_warning'
        else:
            data_reception_status = 'ok'
        
        return data_reception_status, data_reception_details

    
    def print_usage_report(
        self, 
        timestamps: List[datetime], 
        max_gap_minutes: int = 5
    ) -> None:
        """
        Print a formatted usage report to console.
        
        This is a utility method for debugging/admin purposes.
        
        Args:
            timestamps: List of datetime objects
            max_gap_minutes: Maximum gap to consider continuous usage
        """
        stats = self.calculate_usage_statistics(timestamps, max_gap_minutes)
        
        print("=" * 50)
        print("WEARABLE DEVICE USAGE REPORT")
        print("=" * 50)
        print(f"\nTotal Usage: {stats['total_hours']:.2f} hours")
        print(f"Average per Day: {stats['average_hours_per_day']:.2f} hours")
        print(f"Days with Usage: {stats['num_days']}")
        print("\nDaily Breakdown:")
        print("-" * 50)
        
        for date, hours in stats['hours_per_day'].items():
            hours_int = int(hours)
            minutes = int((hours - hours_int) * 60)
            print(f"  {date}: {hours:.2f} hours ({hours_int}h {minutes}m)")
        
        print("=" * 50)

    def get_sleep_night(self, device_id: int, night: Optional[date] = None) -> Optional[Dict[str, Any]]:
        """
        Sleep of one night, shaped for the hypnogram, with links to the
        neighbouring nights.

        A night is identified by the day its sleep ENDS on (DISPLAY_TIMEZONE),
        as in the sleep trend: the night between Monday and Tuesday is Tuesday.
        The longest main sleep ending that day is shown; if there is none, the
        longest nap. Other sleeps ending that day are listed as naps.

        Without `night`, the most recent night that has sleep stage data is
        used (or the most recent night at all, if none has stages).

        Returns None when the device has no sleep at all, otherwise:
            {'night': date,
             'session': <see _sleep_session_view> | None,  # None: nothing that night
             'naps': [{'start', 'end', 'minutes_asleep'}],
             'prev_night', 'next_night': date | None}   # nearest nights with sleep
        """
        if night is None:
            latest = (
                self.sleep_repo.get_nearest_sleep_log(device_id, with_levels=True)
                or self.sleep_repo.get_nearest_sleep_log(device_id)
            )
            if not latest:
                return None
            night = self._utc_to_display(latest.end_time).date()

        day_start = self._display_to_utc(datetime.combine(night, datetime.min.time()))
        day_end = self._display_to_utc(datetime.combine(night + timedelta(days=1), datetime.min.time()))

        # Logs sharing a start time are duplicates: keep the first of each.
        logs, seen_starts = [], set()
        for log in self.sleep_repo.get_sleep_logs_ending_between(device_id, day_start, day_end):
            if log.start_time not in seen_starts:
                seen_starts.add(log.start_time)
                logs.append(log)

        mains = [log for log in logs if log.is_main_sleep]
        shown = max(mains or logs, key=lambda log: log.minutes_asleep or 0, default=None)

        prev_log = self.sleep_repo.get_nearest_sleep_log(device_id, ending_before=day_start)
        next_log = self.sleep_repo.get_nearest_sleep_log(device_id, ending_from=day_end)

        return {
            "night": night,
            "session": self._sleep_session_view(shown) if shown else None,
            "naps": [
                {
                    "start": self._utc_to_display(log.start_time),
                    "end": self._utc_to_display(log.end_time),
                    "minutes_asleep": log.minutes_asleep or 0,
                }
                for log in logs if log is not shown
            ],
            "prev_night": self._utc_to_display(prev_log.end_time).date() if prev_log else None,
            "next_night": self._utc_to_display(next_log.end_time).date() if next_log else None,
        }

    def _sleep_session_view(self, log) -> Optional[Dict[str, Any]]:
        """
        One sleep log shaped for the hypnogram. Sleep timestamps are stored in
        UTC and returned in DISPLAY_TIMEZONE.

        Returns None when the log has no duration, otherwise:
            {'start', 'end': datetime, 'is_main_sleep': bool,
             'minutes_asleep', 'minutes_awake', 'minutes_in_bed': int,
             'rows': [stage, ...] top to bottom,
             'stages': [{'key', 'minutes', 'percent'}] in row order,
             'segments': [{'stage', 'start', 'end', 'minutes', 'left', 'width'}],
             'ticks': [{'label', 'left'}]}
        'left' and 'width' are percentages of the session duration.
        """
        start = self._utc_to_display(log.start_time)
        end = self._utc_to_display(log.end_time)
        total_seconds = (end - start).total_seconds()
        if total_seconds <= 0:
            return None

        def percent(dt: datetime) -> float:
            return round((dt - start).total_seconds() / total_seconds * 100, 3)

        segments = []
        seconds_by_stage = defaultdict(float)
        for level in self.sleep_repo.get_sleep_levels(log.sleep_session_id):
            if level.level not in SLEEP_STAGE_ORDER or level.seconds <= 0:
                continue
            level_start = self._utc_to_display(level.time)
            seg_start = max(level_start, start)
            seg_end = min(level_start + timedelta(seconds=level.seconds), end)
            if seg_end <= seg_start:
                continue
            seconds = (seg_end - seg_start).total_seconds()
            seconds_by_stage[level.level] += seconds
            segments.append({
                "stage": level.level,
                "start": seg_start,
                "end": seg_end,
                "minutes": round(seconds / 60),
                "left": percent(seg_start),
                "width": round(seconds / total_seconds * 100, 3),
            })

        present = set(SLEEP_DEFAULT_STAGES.get(log.type, ())) | set(seconds_by_stage)
        rows = [stage for stage in SLEEP_STAGE_ORDER if stage in present]

        staged_seconds = sum(seconds_by_stage.values())
        stages = [
            {
                "key": stage,
                "minutes": round(seconds_by_stage[stage] / 60),
                "percent": round(seconds_by_stage[stage] / staged_seconds * 100) if staged_seconds else 0,
            }
            for stage in rows
        ]

        # Axis ticks on full hours (every 2 hours for sessions longer than 10h)
        step = timedelta(hours=1 if total_seconds <= 10 * 3600 else 2)
        tick = start.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        ticks = []
        while tick < end:
            ticks.append({"label": tick.strftime("%H:%M"), "left": percent(tick)})
            tick += step

        return {
            "start": start,
            "end": end,
            "is_main_sleep": log.is_main_sleep,
            "minutes_asleep": log.minutes_asleep,
            "minutes_awake": log.minutes_awake,
            "minutes_in_bed": log.minutes_in_the_bed,
            "rows": rows,
            "stages": stages,
            "segments": segments,
            "ticks": ticks,
        }

    def get_sleep_trend(
        self,
        device_id: int,
        num_days: int,
        now: Optional[datetime] = None,
        end_day: Optional[date] = None,
    ) -> Dict[str, Any]:
        """
        Time asleep per day over `num_days` days ending on `end_day` (included;
        default today).

        Each sleep is assigned to the day it ENDS on (in DISPLAY_TIMEZONE), so
        the night between Monday and Tuesday counts for Tuesday. Naps ending the
        same day are added to the total and reported separately. Logs sharing
        the same start time are counted once.

        Returns:
            {'days': [{'date', 'minutes', 'main_minutes', 'nap_minutes',
                       'bed_time', 'wake_time', 'height'}],   # oldest first
             'axis_hours': [0, 2, ...],    # y gridlines, top value = 100% height
             'average_minutes': int | None,  # over days with data
             'days_with_data': int}
        'height' is a percentage of the y-axis maximum; bed/wake times refer
        to the main sleep and are None if there is none.
        """
        today = end_day or (now or self._utc_to_display(datetime.now(timezone.utc))).date()
        first_day = today - timedelta(days=num_days - 1)

        # Logs are filtered on start time (UTC): reach back one extra day for
        # sleeps that started the evening before the first day (and stop one
        # day after the last one; end dates are checked again below).
        query_start = datetime.combine(first_day - timedelta(days=1), datetime.min.time())
        query_end = datetime.combine(today + timedelta(days=1), datetime.min.time())
        logs = self.sleep_repo.get_sleep_logs(device_id, start_date=query_start, end_date=query_end)

        per_day = defaultdict(lambda: {"main": [], "naps": []})
        seen_starts = set()
        for log in logs:
            if log.start_time in seen_starts:
                continue
            seen_starts.add(log.start_time)
            start = self._utc_to_display(log.start_time)
            end = self._utc_to_display(log.end_time)
            if not (first_day <= end.date() <= today):
                continue
            per_day[end.date()]["main" if log.is_main_sleep else "naps"].append((start, end, log.minutes_asleep or 0))

        days = []
        for offset in range(num_days):
            day = first_day + timedelta(days=offset)
            entry = per_day.get(day)
            if not entry:
                days.append({"date": day, "minutes": None, "main_minutes": 0, "nap_minutes": 0,
                             "bed_time": None, "wake_time": None, "height": 0})
                continue
            main_minutes = sum(m for _, _, m in entry["main"])
            nap_minutes = sum(m for _, _, m in entry["naps"])
            main = max(entry["main"], key=lambda s: s[2]) if entry["main"] else None
            days.append({
                "date": day,
                "minutes": main_minutes + nap_minutes,
                "main_minutes": main_minutes,
                "nap_minutes": nap_minutes,
                "bed_time": main[0] if main else None,
                "wake_time": main[1] if main else None,
                "height": 0,
            })

        # Y axis in whole hours, at least 0-10h; gridline step grows with the
        # scale so an outlier never produces more than ~6 gridlines.
        with_data = [d["minutes"] for d in days if d["minutes"] is not None]
        max_hours = -(-max(with_data, default=0) // 60)
        step = 2 if max_hours <= 12 else (4 if max_hours <= 24 else 6)
        top_hours = max(10, -(-max_hours // step) * step)
        for d in days:
            if d["minutes"]:
                d["height"] = round(d["minutes"] / (top_hours * 60) * 100, 2)

        return {
            "days": days,
            "axis_hours": list(range(0, top_hours + 1, step)),
            "average_minutes": round(sum(with_data) / len(with_data)) if with_data else None,
            "days_with_data": len(with_data),
        }

    @classmethod
    def today(cls) -> date:
        """Current date in DISPLAY_TIMEZONE."""
        return cls._utc_to_display(datetime.now(timezone.utc)).date()

    def get_vital_series(
        self,
        device_id: int,
        metric: str,
        start_date: date,
        end_date: date,
        resolution: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Time series and summary of one vital sign from `start_date` to
        `end_date` (both included; callers validate the range).

        metric: 'heart_rate' | 'steps' | 'calories' (intraday, wearer's local
                time) or 'spo2' (sleep SpO2 samples, stored in UTC).
        resolution: one of vital_resolutions(metric, single_day); anything
                else falls back to the first (default) one.

        Returns:
            {'metric', 'start_date', 'end_date',
             'resolutions': [...],  # the options for this metric and range
             'granularity': 'minute' | 'hour' | 'sample' | 'day',
             'points': [{'label': str | None, 'date': date | None,
                         'value': float | None, 'low': float | None, 'high': float | None}],
             'summary': None (no data) or
                 heart_rate/spo2: {'average', 'min', 'max'}
                 steps/calories:  {'total', 'daily_average', 'days_with_data'}}
        Day points carry 'date' and no label; the others carry an 'HH:MM'
        label, plus 'date' for hourly points over several days.
        'low'/'high' (min/max per day or hour) are set only for heart_rate and
        spo2. Points without data have value None, so gaps stay visible.
        """
        num_days = (end_date - start_date).days + 1
        start = datetime.combine(start_date, datetime.min.time())
        end = datetime.combine(end_date + timedelta(days=1), datetime.min.time())
        resolutions = self.vital_resolutions(metric, num_days == 1)
        granularity = resolution if resolution in resolutions else resolutions[0]

        if metric == "spo2":
            points, summary = self._spo2_series(device_id, start, end, granularity, num_days)
        else:
            points, summary = self._intraday_series(device_id, metric, start, end, num_days, granularity)

        return {
            "metric": metric,
            "resolutions": resolutions,
            "start_date": start_date,
            "end_date": end_date,
            "granularity": granularity,
            "points": points,
            "summary": summary,
        }

    @staticmethod
    def vital_resolutions(metric: str, single_day: bool) -> List[str]:
        """Chart resolutions available for a metric; the first one is the default."""
        if not single_day:
            return ["day", "hour"]
        return {"heart_rate": ["minute", "hour"], "spo2": ["sample", "hour"]}.get(metric, ["hour"])

    @staticmethod
    def _empty_point(bucket_start: datetime, granularity: str, num_days: int) -> Dict[str, Any]:
        """Chart point for a bucket: day -> date; minute/hour -> 'HH:MM' (+ date over several days)."""
        return {
            "label": None if granularity == "day" else bucket_start.strftime("%H:%M"),
            "date": bucket_start.date() if granularity == "day" or num_days > 1 else None,
            "value": None,
            "low": None,
            "high": None,
        }

    def _intraday_series(self, device_id, metric, start, end, num_days, granularity):
        rows = self.metrics_repo.get_intraday_buckets(device_id, metric, start, end, granularity)
        # Intraday times are the wearer's wall-clock time: drop any tzinfo
        by_bucket = {r[0].replace(tzinfo=None): r for r in rows}

        step = {"minute": timedelta(minutes=1), "hour": timedelta(hours=1), "day": timedelta(days=1)}[granularity]
        count = {"minute": 1440 * num_days, "hour": 24 * num_days, "day": num_days}[granularity]
        is_average = metric == "heart_rate"

        points = []
        for i in range(count):
            bucket_start = start + i * step
            row = by_bucket.get(bucket_start)
            point = self._empty_point(bucket_start, granularity, num_days)
            if row:
                _, avg, low, high, total, _ = row
                point["value"] = round(avg if is_average else total, 1)
                if is_average and granularity in ("day", "hour"):
                    point["low"], point["high"] = round(low), round(high)
            points.append(point)

        if not rows:
            return points, None

        if is_average:
            samples = sum(r[5] for r in rows)
            summary = {
                "average": round(sum(r[1] * r[5] for r in rows) / samples),
                "min": round(min(r[2] for r in rows)),
                "max": round(max(r[3] for r in rows)),
            }
        else:
            total = sum(r[4] for r in rows)
            days_with_data = len({r[0].replace(tzinfo=None).date() for r in rows})
            summary = {
                "total": round(total),
                "daily_average": round(total / days_with_data),
                "days_with_data": days_with_data,
            }
        return points, summary

    def _spo2_series(self, device_id, start, end, granularity, num_days):
        samples = []
        seen = set()
        for time_utc, value in self.sleep_repo.get_spo2_samples(
            device_id, self._display_to_utc(start), self._display_to_utc(end)
        ):
            if time_utc in seen:  # sessions duplicated by the old collector bug
                continue
            seen.add(time_utc)
            samples.append((self._utc_to_display(time_utc), value))

        if granularity == "sample":
            points = [
                {"label": t.strftime("%H:%M"), "date": None, "value": round(v, 1), "low": None, "high": None}
                for t, v in samples
            ]
        else:
            # Average, min and max per day or per hour
            step = timedelta(days=1) if granularity == "day" else timedelta(hours=1)
            by_bucket = defaultdict(list)
            for t, v in samples:
                key = t.replace(hour=0 if granularity == "day" else t.hour, minute=0, second=0, microsecond=0)
                by_bucket[key].append(v)
            points = []
            bucket_start = start
            while bucket_start < end:
                point = self._empty_point(bucket_start, granularity, num_days)
                values = by_bucket.get(bucket_start)
                if values:
                    point["value"] = round(sum(values) / len(values), 1)
                    point["low"] = round(min(values), 1)
                    point["high"] = round(max(values), 1)
                points.append(point)
                bucket_start += step

        if not samples:
            return points, None

        values = [v for _, v in samples]
        summary = {
            "average": round(sum(values) / len(values), 1),
            "min": round(min(values), 1),
            "max": round(max(values), 1),
        }
        return points, summary

    @staticmethod
    def _display_to_utc(dt: datetime) -> datetime:
        """Naive wall-clock time in DISPLAY_TIMEZONE -> naive UTC."""
        return dt.replace(tzinfo=_DISPLAY_TZ).astimezone(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _utc_to_display(dt: datetime) -> datetime:
        """Naive UTC (or aware) datetime -> naive wall-clock time in DISPLAY_TIMEZONE."""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(_DISPLAY_TZ).replace(tzinfo=None)
