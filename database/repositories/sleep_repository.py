from typing import Optional, List, Dict, Any
from datetime import datetime, date
from database.connection import ConnectionManager
from database.models import SleepSession, SleepLog, SleepLevel


class SleepRepository:
    """
    Repository for sleep data operations.
    
    Handles sleep sessions, logs, levels, and short levels.
    """
    
    def __init__(self, connection_manager: ConnectionManager):
        """
        Initialize the repository with a connection manager.
        
        Args:
            connection_manager: Active ConnectionManager instance
        """
        self.db = connection_manager

    # ===== Sleep Sessions =====
    
    def create_session(self, device_id: int) -> Optional[int]:
        """
        Create a new sleep session grouping for inserting logs.

        Args:
            device_id: Which device the sleep belongs to.

        Returns:
            int: The new sleep session ID, or None on failure
        """
        query = """
            INSERT INTO sleep_sessions (device_id) 
            VALUES (%s)
            RETURNING id
        """
        result = self.db.execute_query(query, (device_id,))
        
        if result:
            session_id = result[0][0]
            print(f"Sleep session {session_id} inserted for device {device_id}")
            return session_id
        return None

    # ===== Sleep Logs =====
    
    def get_sleep_logs(
        self,
        device_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[SleepLog]:
        """
        Fetch sleep log entries for a device across an optional date range.

        Args:
            device_id: Device identifier.
            start_date: Only include logs after this time.
            end_date: Only include logs before this time.

        Returns:
            List of SleepLog objects ordered by start time descending.
        """
        query = """
            SELECT sl.id, sl.sleep_session_id, sl.start_time, sl.end_time,
                   sl.is_main_sleep, sl.duration, sl.minutes_asleep,
                   sl.minutes_awake, sl.minutes_in_the_bed, sl.log_type, sl.type
            FROM sleep_logs sl
            JOIN sleep_sessions ss ON sl.sleep_session_id = ss.id
            WHERE ss.device_id = %s
        """
        params = [device_id]

        if start_date:
            query += " AND sl.start_time >= %s"
            params.append(start_date)

        if end_date:
            query += " AND sl.start_time <= %s"
            params.append(end_date)

        query += " ORDER BY sl.start_time DESC"

        result = self.db.execute_query(query, tuple(params))
        
        if result:
            return [
                SleepLog(
                    id=row[0],
                    sleep_session_id=row[1],
                    start_time=row[2],
                    end_time=row[3],
                    is_main_sleep=row[4],
                    duration=row[5],
                    minutes_asleep=row[6],
                    minutes_awake=row[7],
                    minutes_in_the_bed=row[8],
                    log_type=row[9],
                    type=row[10]
                )
                for row in result
            ]
        return []

    def insert_sleep_log(self, sleep_session_id: int, data: Dict[str, Any]) -> bool:
        """
        Persist a detailed sleep segment.

        Args:
            sleep_session_id: The parent sleep session.
            data: Sleep fields from an external API.

        Returns:
            bool: True on success.
        """
        query = """
            INSERT INTO sleep_logs (
                sleep_session_id, start_time, end_time, is_main_sleep, duration, 
                minutes_asleep, minutes_awake, minutes_in_the_bed, log_type, type
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        result = self.db.execute_query(query, (
            sleep_session_id,
            data['startTime'], 
            data['endTime'],
            data['isMainSleep'],
            data['duration'] / 1000,  # Convert milliseconds to seconds
            data['minutesAsleep'],
            data['minutesAwake'],
            data['timeInBed'],
            data['logType'],
            data['type']
        ))

        if result:
            print(f"Sleep log inserted for sleep session {sleep_session_id}")
        return bool(result)

    # ===== Sleep Levels =====
    
    def get_sleep_levels(self, sleep_session_id: int) -> List[SleepLevel]:
        """
        Get all sleep levels for a given session.

        Args:
            sleep_session_id: The sleep session identifier

        Returns:
            List of SleepLevel objects
        """
        query = """
            SELECT id, sleep_session_id, time, level, seconds
            FROM sleep_levels
            WHERE sleep_session_id = %s
            ORDER BY time
        """
        result = self.db.execute_query(query, (sleep_session_id,))
        
        if result:
            return [
                SleepLevel(
                    id=row[0],
                    sleep_session_id=row[1],
                    time=row[2],
                    level=row[3],
                    seconds=row[4]
                )
                for row in result
            ]
        return []

    def insert_sleep_level(self, sleep_session_id: int, data: Dict[str, Any]) -> bool:
        """
        Insert a detailed sleep level entry (e.g., REM, deep, awake).

        Args:
            sleep_session_id: Parent session.
            data: Level info with keys: dateTime, level, seconds

        Returns:
            bool: True on success.
        """
        query = """
            INSERT INTO sleep_levels (
                sleep_session_id, time, level, seconds
            ) 
            VALUES (%s, %s, %s, %s)
        """
        result = self.db.execute_query(query, (
            sleep_session_id, 
            data['dateTime'], 
            data['level'],
            data['seconds']
        ))

        if result:
            print(f"Sleep level record inserted for sleep session {sleep_session_id}")
        return bool(result)

    def insert_sleep_short_level(
        self, 
        sleep_session_id: int, 
        short: Dict[str, Any]
    ) -> bool:
        """
        Persist a "short level" segment within a sleep session.

        Args:
            sleep_session_id: Parent session.
            short: Entry data with keys: dateTime, seconds

        Returns:
            bool: True on success.
        """
        query = """
            INSERT INTO sleep_short_levels (
                sleep_session_id, time, seconds
            ) 
            VALUES (%s, %s, %s)
        """
        result = self.db.execute_query(query, (
            sleep_session_id, 
            short['dateTime'],
            short['seconds']
        ))

        if result:
            print(f"Sleep short level record inserted for sleep session {sleep_session_id}")
        return bool(result)

    # ===== Batch Operations =====
    
    def insert_complete_sleep_data(
        self, 
        device_id: int, 
        sleep_data: Dict[str, Any]
    ) -> Optional[int]:
        """
        Insert a complete sleep record with session, log, levels, and short levels.

        This is a convenience method that creates a session and all related data
        in one call.

        Args:
            device_id: The device this sleep data belongs to
            sleep_data: Complete sleep data from external API

        Returns:
            int: The sleep session ID if successful, None otherwise
        """
        # Create session
        session_id = self.create_session(device_id)
        if not session_id:
            return None

        # Insert main log
        if not self.insert_sleep_log(session_id, sleep_data):
            return None

        # Insert levels if present
        if 'levels' in sleep_data and 'data' in sleep_data['levels']:
            for level_data in sleep_data['levels']['data']:
                self.insert_sleep_level(session_id, level_data)

        # Insert short levels if present
        if 'levels' in sleep_data and 'shortData' in sleep_data['levels']:
            for short_data in sleep_data['levels']['shortData']:
                self.insert_sleep_short_level(session_id, short_data)

        return session_id
