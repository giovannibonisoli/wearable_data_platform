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

from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Any, Optional

from database import ConnectionManager, DeviceRepository, MetricsRepository, Device 


class DeviceStatisticsService:
    """
    Service for calculating device usage statistics.
    
    This service encapsulates business logic for analyzing device usage
    patterns, sync status, and data gaps.
    """
    
    def __init__(self, connection_manager: ConnectionManager):
        """
        Initialize the service with a connection manager.
        
        Args:
            connection_manager: Active ConnectionManager instance
        """
        self.conn = connection_manager
        self.device_repo = DeviceRepository(connection_manager)
        self.metrics_repo = MetricsRepository(connection_manager)


    def get_device_sync_data(self, device_id: int):
        data_reception_details = {}
        data_reception_status = 'no_data'

        try:

            last_sync = device_repo.get_last_synch(device_id)
            now = datetime.now()

            last_sync = last_sync.replace(tzinfo=now.tzinfo)
            data_reception_details['sync_days'] = (now - last_sync).days
            data_reception_details['sync_hours'] = (now - last_sync).seconds // 3600
            data_reception_details['sync_minutes'] = (now - last_sync).seconds // 60

            intraday_checkpoint = device_repo.get_intraday_checkpoint(device_id)

            if intraday_checkpoint:
                intraday_checkpoint = intraday_checkpoint.replace(tzinfo=last_sync.tzinfo)
                data_reception_details['gap_days'] = max((last_sync - intraday_checkpoint).days, 0)
                        
            else:
                data_reception_details['gap_days'] = 0
                                    
                # Determine overall status
            if data_reception_details['sync_days'] > 7:
                data_reception_status = 'sync_warning'
            else:
                if data_reception_details['gap_days'] > 3:
                    data_reception_status = 'gap_warning' 
                else:
                    data_reception_status = 'ok'

            return data_reception_status, data_reception_details

        except Exception as e:
            error_msg = f"Error while computing data reception details: {e}"
        
            raise Exception(error_msg)
    
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


