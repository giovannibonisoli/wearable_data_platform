from typing import Optional, List, Dict, Any
from datetime import datetime, date
from database.connection import ConnectionManager
from database.models import DailySummary, IntradayMetric


class MetricsRepository:
    """
    Repository for health metrics operations.
    
    Handles daily summaries and intraday (time-series) health data.
    """
    
    def __init__(self, connection_manager: ConnectionManager):
        """
        Initialize the repository with a connection manager.
        
        Args:
            connection_manager: Active ConnectionManager instance
        """
        self.db = connection_manager

    # ===== Daily Summaries =====
    
    def get_daily_summaries(
        self,
        device_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[DailySummary]:
        """
        Retrieve daily summary records for a specific device within an optional date range.

        Args:
            device_id: Identifier for the device whose summaries to fetch.
            start_date: Include summaries on/after this date.
            end_date: Include summaries on/before this date.

        Returns:
            List of DailySummary objects chronologically ordered.
        """
        query = """
            SELECT id, device_id, date, steps, heart_rate, sleep_minutes,
                   calories, distance, floors, elevation, active_minutes,
                   sedentary_minutes, nutrition_calories, water, weight,
                   bmi, fat, oxygen_saturation, respiratory_rate, temperature
            FROM daily_summaries
            WHERE device_id = %s
        """
        params = [device_id]

        if start_date:
            query += " AND date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND date <= %s"
            params.append(end_date)

        query += " ORDER BY date ASC"

        result = self.db.execute_query(query, tuple(params))
        
        if result:
            return [
                DailySummary(
                    id=row[0],
                    device_id=row[1],
                    date=row[2],
                    steps=row[3],
                    heart_rate=row[4],
                    sleep_minutes=row[5],
                    calories=row[6],
                    distance=row[7],
                    floors=row[8],
                    elevation=row[9],
                    active_minutes=row[10],
                    sedentary_minutes=row[11],
                    nutrition_calories=row[12],
                    water=row[13],
                    weight=row[14],
                    bmi=row[15],
                    fat=row[16],
                    oxygen_saturation=row[17],
                    respiratory_rate=row[18],
                    temperature=row[19]
                )
                for row in result
            ]
        return []

    def insert_daily_summary(
        self, 
        device_id: int, 
        date_value: date, 
        **data
    ) -> bool:
        """
        Insert or update a daily summary in the daily_summaries table.

        Uses ON CONFLICT to update existing records.

        Args:
            device_id: The device identifier
            date_value: The date for this summary
            **data: Keyword arguments for metric values (steps, heart_rate, etc.)

        Returns:
            bool: True on success
        """
        query = """
            INSERT INTO daily_summaries (
                device_id, date, steps, heart_rate, sleep_minutes,
                calories, distance, floors, elevation, active_minutes,
                sedentary_minutes, nutrition_calories, water, weight,
                bmi, fat, oxygen_saturation, respiratory_rate, temperature
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (device_id, date) DO UPDATE SET
                steps = EXCLUDED.steps,
                heart_rate = EXCLUDED.heart_rate,
                sleep_minutes = EXCLUDED.sleep_minutes,
                calories = EXCLUDED.calories,
                distance = EXCLUDED.distance,
                floors = EXCLUDED.floors,
                elevation = EXCLUDED.elevation,
                active_minutes = EXCLUDED.active_minutes,
                sedentary_minutes = EXCLUDED.sedentary_minutes,
                nutrition_calories = EXCLUDED.nutrition_calories,
                water = EXCLUDED.water,
                weight = EXCLUDED.weight,
                bmi = EXCLUDED.bmi,
                fat = EXCLUDED.fat,
                oxygen_saturation = EXCLUDED.oxygen_saturation,
                respiratory_rate = EXCLUDED.respiratory_rate,
                temperature = EXCLUDED.temperature
        """
        
        result = self.db.execute_query(query, (
            device_id, 
            date_value,
            data.get("steps"),
            data.get("heart_rate"),
            data.get("sleep_minutes"),
            data.get("calories"),
            data.get("distance"),
            data.get("floors"),
            data.get("elevation"),
            data.get("active_minutes"),
            data.get("sedentary_minutes"),
            data.get("nutrition_calories"),
            data.get("water"),
            data.get("weight"),
            data.get("bmi"),
            data.get("fat"),
            data.get("oxygen_saturation"),
            data.get("respiratory_rate"),
            data.get("temperature")
        ))
        return bool(result)

    def get_device_history(self, device_id: int) -> List[DailySummary]:
        """
        Return full history of daily summaries for a device.

        Args:
            device_id: The device identifier.

        Returns:
            List of DailySummary objects ordered by date.
        """
        return self.get_daily_summaries(device_id)

    # ===== Intraday Metrics =====
    
    def get_intraday_metrics(
        self,
        device_id: int,
        metric_type: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[tuple]:
        """
        Fetch intraday (timestamped) metric records for a specific device.

        Args:
            device_id: The device identifier.
            metric_type: Column name representing the metric (e.g., 'heart_rate').
            start_time: Only include records after this timestamp.
            end_time: Only include records before this timestamp.

        Returns:
            List of (time, value) tuples for the requested metric.
        """
        query = f"""
            SELECT time, {metric_type} 
            FROM intraday_metrics
            WHERE device_id = %s AND {metric_type} IS NOT NULL
        """
        params = [device_id]

        if start_time:
            query += " AND time >= %s"
            params.append(start_time)

        if end_time:
            query += " AND time <= %s"
            params.append(end_time)

        query += " ORDER BY time"

        result = self.db.execute_query(query, tuple(params))
        return result if result else []

    def check_intraday_timestamp_exists(
        self, 
        device_id: int, 
        timestamp: datetime
    ) -> bool:
        """
        Determine if a given timestamp already exists for intraday data.

        Args:
            device_id: The device to check.
            timestamp: The specific timestamp.

        Returns:
            bool: True if exists.
        """
        query = """
            SELECT 1 FROM intraday_metrics
            WHERE device_id = %s AND time = %s
            LIMIT 1
        """
        result = self.db.execute_query(query, (device_id, timestamp))
        return bool(result)

    def insert_intraday_metric(
        self,
        device_id: int,
        timestamp: datetime,
        data_type: str = "heart_rate",
        value: Optional[float] = None,
    ) -> bool:
        """
        Save or update a metric value at a particular timestamp.

        If a row exists, updates just the given column; otherwise
        inserts a new record with other fields NULL.

        Args:
            device_id: Device identifier.
            timestamp: Exact capture time.
            data_type: Which intraday column (e.g., 'steps', 'heart_rate').
            value: The measured value.

        Returns:
            bool: True on success.
        """
        if self.check_intraday_timestamp_exists(device_id, timestamp):
            # Update existing record
            query = f"""
                UPDATE intraday_metrics
                SET {data_type} = %s
                WHERE device_id = %s AND time = %s
            """
            result = self.db.execute_query(query, (value, device_id, timestamp))
            if result:
                print(f"Intraday {data_type} data for device {device_id} successfully updated.")
            return bool(result)
        else:
            # Insert new record
            values = {
                "heart_rate": None,
                "steps": None,
                "calories": None,
                "distance": None
            }
            values[data_type] = value

            query = """
                INSERT INTO intraday_metrics (device_id, time, heart_rate, steps, calories, distance)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            result = self.db.execute_query(query, (
                device_id, 
                timestamp, 
                values["heart_rate"], 
                values["steps"], 
                values["calories"], 
                values["distance"]
            ))
            if result:
                print(f"Intraday {data_type} data for device {device_id} successfully inserted.")
            return bool(result)

    def get_intraday_timestamps_by_range(
        self, 
        device_id: int, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[datetime]:
        """
        Get all intraday data timestamps within a date range.

        Args:
            device_id: The device identifier
            start_date: Start of the range
            end_date: End of the range

        Returns:
            List of datetime objects
        """
        query = """
            SELECT time 
            FROM intraday_metrics 
            WHERE device_id = %s AND time > %s AND time < %s 
            ORDER BY time
        """
        result = self.db.execute_query(query, (device_id, start_date, end_date))
        return [row[0] for row in result] if result else []
