from typing import Optional, List, Dict, Any
from datetime import datetime
from database.connection import ConnectionManager
from database.models import Alert


class AlertRepository:
    """
    Repository for alert operations.
    
    Handles creation, retrieval, and management of health alerts.
    """
    
    def __init__(self, connection_manager: ConnectionManager):
        """
        Initialize the repository with a connection manager.
        
        Args:
            connection_manager: Active ConnectionManager instance
        """
        self.db = connection_manager

    def get_alerts(
        self,
        email_id: int,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        acknowledged: Optional[bool] = None,
    ) -> List[Alert]:
        """
        Retrieve alert events for a device.

        Args:
            email_id: Device/email identifier.
            start_time: Only include alerts after this.
            end_time: Only include alerts before this.
            acknowledged: Filter by acknowledgment status (True/False/None for all).

        Returns:
            List of Alert objects ordered by most recent first.
        """
        query = """
            SELECT id, email_id, alert_type, priority, triggering_value,
                   threshold_value, alert_time, details, acknowledged
            FROM alerts
            WHERE email_id = %s
        """
        params = [email_id]

        if start_time:
            query += " AND alert_time >= %s"
            params.append(start_time)
        if end_time:
            query += " AND alert_time <= %s"
            params.append(end_time)
        if acknowledged is not None:
            query += " AND acknowledged = %s"
            params.append(acknowledged)

        query += " ORDER BY alert_time DESC"

        result = self.db.execute_query(query, tuple(params))
        
        if result:
            return [
                Alert(
                    id=row[0],
                    email_id=row[1],
                    alert_type=row[2],
                    priority=row[3],
                    triggering_value=row[4],
                    threshold_value=row[5],
                    alert_time=row[6],
                    details=row[7],
                    acknowledged=row[8]
                )
                for row in result
            ]
        return []

    def get_by_id(self, alert_id: int) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific alert and associated user details.

        Args:
            alert_id: The alert's primary key.

        Returns:
            dict with alert fields + user name/email if found, None otherwise.
        """
        query = """
            SELECT a.id, a.email_id, a.alert_type, a.priority, 
                   a.triggering_value, a.threshold_value, a.alert_time,
                   a.details, a.acknowledged, u.name as user_name, u.email as user_email
            FROM alerts a
            JOIN users u ON a.user_id = u.id
            WHERE a.id = %s
        """
        result = self.db.execute_query(query, (alert_id,))
        
        if result and len(result) > 0:
            columns = [desc[0] for desc in self.db.cursor.description]
            alert = dict(zip(columns, result[0]))
            return alert
        return None

    def create(
        self,
        email_id: int,
        alert_type: str,
        priority: str,
        triggering_value: float,
        threshold: Any,
        timestamp: Optional[datetime] = None,
        details: Optional[str] = None
    ) -> Optional[int]:
        """
        Create a new alert record.

        Args:
            email_id: Device/email identifier.
            alert_type: A descriptive alert category.
            priority: One of 'high', 'medium', or 'low'.
            triggering_value: The value that triggered the alert condition.
            threshold: The threshold definition (converted to string).
            timestamp: When the alert occurred (defaults to now).
            details: Additional context.

        Returns:
            int: The new alert's ID on success, None otherwise.
        """
        if timestamp is None:
            timestamp = datetime.now()

        # Convert threshold to string if it isn't already
        threshold_str = str(threshold)

        query = """
            INSERT INTO alerts (
                email_id, alert_type, priority, triggering_value, 
                threshold_value, alert_time, details
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        
        result = self.db.execute_query(query, (
            email_id, 
            alert_type, 
            priority, 
            triggering_value, 
            threshold_str, 
            timestamp, 
            details
        ))
        
        return result[0][0] if result else None

    def acknowledge(self, alert_id: int) -> bool:
        """
        Mark an alert as acknowledged.

        Args:
            alert_id: The alert to acknowledge

        Returns:
            bool: True if successful
        """
        query = """
            UPDATE alerts
            SET acknowledged = TRUE
            WHERE id = %s
        """
        result = self.db.execute_query(query, (alert_id,))
        return bool(result)

    def unacknowledge(self, alert_id: int) -> bool:
        """
        Mark an alert as unacknowledged.

        Args:
            alert_id: The alert to unacknowledge

        Returns:
            bool: True if successful
        """
        query = """
            UPDATE alerts
            SET acknowledged = FALSE
            WHERE id = %s
        """
        result = self.db.execute_query(query, (alert_id,))
        return bool(result)

    def delete(self, alert_id: int) -> bool:
        """
        Delete an alert.

        Args:
            alert_id: The alert to delete

        Returns:
            bool: True if successful
        """
        query = "DELETE FROM alerts WHERE id = %s"
        result = self.db.execute_query(query, (alert_id,))
        return bool(result)

    def get_unacknowledged_count(self, email_id: int) -> int:
        """
        Get the count of unacknowledged alerts for a device.

        Args:
            email_id: Device/email identifier

        Returns:
            int: Number of unacknowledged alerts
        """
        query = """
            SELECT COUNT(*) 
            FROM alerts 
            WHERE email_id = %s AND acknowledged = FALSE
        """
        result = self.db.execute_query(query, (email_id,))
        return result[0][0] if result else 0

    def get_by_priority(
        self, 
        email_id: int, 
        priority: str, 
        acknowledged: Optional[bool] = None
    ) -> List[Alert]:
        """
        Get alerts filtered by priority level.

        Args:
            email_id: Device/email identifier
            priority: Priority level ('high', 'medium', 'low')
            acknowledged: Optional filter by acknowledgment status

        Returns:
            List of Alert objects
        """
        query = """
            SELECT id, email_id, alert_type, priority, triggering_value,
                   threshold_value, alert_time, details, acknowledged
            FROM alerts
            WHERE email_id = %s AND priority = %s
        """
        params = [email_id, priority]

        if acknowledged is not None:
            query += " AND acknowledged = %s"
            params.append(acknowledged)

        query += " ORDER BY alert_time DESC"

        result = self.db.execute_query(query, tuple(params))
        
        if result:
            return [
                Alert(
                    id=row[0],
                    email_id=row[1],
                    alert_type=row[2],
                    priority=row[3],
                    triggering_value=row[4],
                    threshold_value=row[5],
                    alert_time=row[6],
                    details=row[7],
                    acknowledged=row[8]
                )
                for row in result
            ]
        return []
