from database import Database, ConnectionManager, DeviceRepository, MetricsRepository
from datetime import datetime, timedelta
from collections import defaultdict


def print_usage_report(timestamps, max_gap_minutes=5):
    """Print a formatted usage report."""
    stats = calculate_usage_statistics(timestamps, max_gap_minutes)
    
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


def calculate_usage_statistics(timestamps, max_gap_minutes=5):
    """
    Calculate usage statistics from wearable device timestamps.
    
    Args:
        timestamps: List of datetime objects
        max_gap_minutes: Maximum gap between timestamps to consider continuous usage
        
    Returns:
        Dictionary containing:
        - 'total_hours': Total usage hours across all days
        - 'average_hours_per_day': Average usage hours per day
        - 'num_days': Number of days with recorded usage
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
        # 'hours_per_day': hours_per_day,
        'total_hours': total_hours,
        'average_hours_per_day': average_hours,
        'num_days': num_days
    }


def get_last_device_usage_statistics(device_id, temporal_range):
    
    with ConnectionManager() as conn:
        device_repo = DeviceRepository(conn)
        metrics_repo = MetricsRepository(conn)
        
        try:
            last_sync = device_repo.get_last_synch(device_id)

            end_date = datetime.now()
            start_date = (end_date - temporal_range)

            start_date = start_date.replace(tzinfo=last_sync.tzinfo)
            
            if last_sync > start_date:
                timestamps = metrics_repo.get_intraday_timestamps_by_range(device_id, start_date, end_date)

                if len(timestamps) > 0:
                    # timestamps = [timestamp[0] for timestamp in timestamps]
                    usage_time_data = calculate_usage_statistics(timestamps)

                    return usage_time_data
                else:
                    return {'total_hours': 0, 'average_hours_per_day': 0, 'num_days': 0}
                
            else:
                return {'total_hours': 0, 'average_hours_per_day': 0, 'num_days': 0}

        except Exception as e:
            error_msg = f"Error while computing device usage statistics: {e}"
        
            raise Exception(error_msg)


def get_device_sync_data(device_id):
    data_reception_details = {}
    data_reception_status = 'no_data'

    with ConnectionManager() as conn:
        device_repo = DeviceRepository(conn)
        metrics_repo = MetricsRepository(conn)

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
    

def compute_device_usage_statistics(device_id):
    pass

    

    # Calcolare il non uso medio

    # Calcolare il ritardo medio di sincronizzazione e gli intervalli medi di dati persi 
    # a causa della mancata sicronizzazione

    # Calcolare l'utilizzo medio consecutivo del device sia in termini di minuti, ore, giorni, settimane e mesi

    # Fare questi calcoli coprendo varie intervalli di tempo (ore, giorni, settimane e mesi)



if __name__ == "__main__":
    try:
        print(get_last_device_usage_statistics(2, timedelta(days=300)))
        print(get_device_sync_data(2))

    except Exception as e:
        print(e)