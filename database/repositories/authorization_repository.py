from typing import Optional, Dict, Any
from datetime import datetime
from database.connection import ConnectionManager
from database.models import PendingAuthorization


class AuthorizationRepository:
    """
    Repository for OAuth authorization operations.
    
    Handles pending authorizations for PKCE flow.
    """
    
    def __init__(self, connection_manager: ConnectionManager):
        """
        Initialize the repository with a connection manager.
        
        Args:
            connection_manager: Active ConnectionManager instance
        """
        self.db = connection_manager

    def store_pending_auth(
        self, 
        device_id: int, 
        state: str, 
        code_verifier: str
    ) -> bool:
        """
        Save a new authorization attempt awaiting completion.

        Args:
            device_id: The device requesting OAuth/consent.
            state: A unique state value for callback correlation.
            code_verifier: Challenge for PKCE flow.

        Returns:
            bool: True on success.
        """
        query = """
            INSERT INTO pending_authorizations (device_id, state, code_verifier, expires_at)
            VALUES (%s, %s, %s, NOW() + INTERVAL '10 minutes')
        """
        result = self.db.execute_query(query, (device_id, state, code_verifier))
        return bool(result)

    def get_by_state(self, state: str) -> Optional[Dict[str, Any]]:
        """
        Fetch an unexpired pending authorization by state.

        Args:
            state: The PKCE callback state.

        Returns:
            dict with 'code_verifier' and 'device_id', or None if not found/expired.
        """
        query = """
            SELECT code_verifier, device_id
            FROM pending_authorizations
            WHERE state = %s AND expires_at > NOW()
        """
        result = self.db.execute_query(query, (state,))
        
        if result:
            return {
                'code_verifier': result[0][0], 
                'device_id': result[0][1]
            }
        return None

    def check_exists(self, device_id: int) -> bool:
        """
        Check existence of an unexpired pending authorization for a device.

        Args:
            device_id: Device with pending authorization.

        Returns:
            bool: True if a pending auth exists and hasn't expired.
        """
        query = """
            SELECT *
            FROM pending_authorizations
            WHERE device_id = %s AND expires_at > NOW()
        """

        result = self.db.execute_query(query, (device_id,))
        return bool(result)

    def delete_by_state(self, state: str) -> bool:
        """
        Remove a pending authorization once used or expired.

        Args:
            state: The pending auth state value.

        Returns:
            bool: True on success.
        """
        query = "DELETE FROM pending_authorizations WHERE state = %s"
        result = self.db.execute_query(query, (state,))
        return bool(result)

    def delete_by_device(self, device_id: int) -> bool:
        """
        Remove all pending authorizations for a device.

        Args:
            device_id: The device identifier

        Returns:
            bool: True on success
        """
        query = "DELETE FROM pending_authorizations WHERE device_id = %s"
        result = self.db.execute_query(query, (device_id,))
        return bool(result)

    def cleanup_expired(self) -> int:
        """
        Remove all expired pending authorizations.

        Returns:
            int: Number of records deleted
        """
        query = "DELETE FROM pending_authorizations WHERE expires_at <= NOW()"
        result = self.db.execute_query(query)
        
        # Get the number of rows affected
        if result and hasattr(self.db.cursor, 'rowcount'):
            return self.db.cursor.rowcount
        return 0

    def get_all_for_device(self, device_id: int) -> list:
        """
        Get all pending authorizations for a device (including expired).

        Useful for debugging.

        Args:
            device_id: The device identifier

        Returns:
            List of dicts with pending auth details
        """
        query = """
            SELECT id, device_id, state, expires_at, created_at
            FROM pending_authorizations
            WHERE device_id = %s
            ORDER BY created_at DESC
        """
        result = self.db.execute_query(query, (device_id,))
        
        if result:
            return [
                {
                    'id': row[0],
                    'device_id': row[1],
                    'state': row[2],
                    'expires_at': row[3],
                    'created_at': row[4],
                    'is_expired': row[3] <= datetime.now()
                }
                for row in result
            ]
        return []
