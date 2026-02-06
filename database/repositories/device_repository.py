from typing import Optional, List, Tuple, Dict, Any
from datetime import datetime, date
from database.connection import ConnectionManager
from database.models import Device
from encryption import encrypt_token, decrypt_token


class DeviceRepository:
    """
    Repository for device operations.
    
    Handles device management, OAuth tokens, and authorization status.
    """
    
    def __init__(self, connection_manager: ConnectionManager):
        """
        Initialize the repository with a connection manager.
        
        Args:
            connection_manager: Active ConnectionManager instance
        """
        self.db = connection_manager

    def create(
        self,
        admin_user_id: int,
        email_address: str,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
    ) -> Optional[int]:
        """
        Insert a new device for an admin user.

        Tokens are encrypted if provided.

        Args:
            admin_user_id: Owner admin user.
            email_address: Unique identifier for the device.
            access_token: Token returned from an external provider.
            refresh_token: Token used to refresh the access_token.

        Returns:
            int: The new device's id if successful, None otherwise.
        """
        if access_token and refresh_token:
            encrypted_access_token = encrypt_token(access_token)
            encrypted_refresh_token = encrypt_token(refresh_token)
        else:
            encrypted_access_token = None
            encrypted_refresh_token = None

        query = """
            INSERT INTO devices (admin_user_id, email_address, authorization_status, device_type, 
                                    daily_summaries_checkpoint, intraday_checkpoint, sleep_checkpoint, 
                                    last_synch, access_token, refresh_token)
            VALUES (%s, %s,'inserted', NULL, NULL, NULL, NULL, NULL, %s, %s)
            RETURNING id
        """

        print("INPUTS:", (admin_user_id, email_address, encrypted_access_token, encrypted_refresh_token))

        result = self.db.execute_query(
            query, 
            (admin_user_id, email_address, encrypted_access_token, encrypted_refresh_token)
        )

        return result[0][0] if result else None

    def get_by_id(self, device_id: int) -> Optional[Device]:
        """
        Fetch a device by ID.

        Args:
            device_id: The device identifier.

        Returns:
            Device object or None if not found.
        """
        query = """
            SELECT id, email_address, authorization_status, admin_user_id, device_type,
                   created_at, last_synch, daily_summaries_checkpoint, 
                   intraday_checkpoint, sleep_checkpoint
            FROM devices
            WHERE id = %s
        """
        result = self.db.execute_query(query, (device_id,))
        
        if result:
            row = result[0]
            return Device(
                id=row[0],
                email_address=row[1],
                authorization_status=row[2],
                admin_user_id=row[3],
                device_type=row[4],
                created_at=row[5],
                last_synch=row[6],
                daily_summaries_checkpoint=row[7],
                intraday_checkpoint=row[8],
                sleep_checkpoint=row[9]
            )
        return None

    def get_by_email(self, email_address: str) -> Optional[Device]:
        """
        Find the latest device record associated with an email.

        Args:
            email_address: The address identifier.

        Returns:
            Device object if found, None otherwise.
        """
        query = """
            SELECT id, email_address, authorization_status, admin_user_id, device_type,
                   created_at, last_synch, daily_summaries_checkpoint, 
                   intraday_checkpoint, sleep_checkpoint
            FROM devices
            WHERE email_address = %s
            ORDER BY created_at DESC
            LIMIT 1
        """
        result = self.db.execute_query(query, (email_address,))
        
        if result:
            row = result[0]
            return Device(
                id=row[0],
                email_address=row[1],
                authorization_status=row[2],
                admin_user_id=row[3],
                device_type=row[4],
                created_at=row[5],
                last_synch=row[6],
                daily_summaries_checkpoint=row[7],
                intraday_checkpoint=row[8],
                sleep_checkpoint=row[9]
            )
        return None

    def get_by_admin_user(self, admin_user_id: int) -> List[Device]:
        """
        List all devices linked to a particular admin user.

        Args:
            admin_user_id: The admin user's primary key.

        Returns:
            List of Device objects sorted by creation date descending.
        """
        query = """
            SELECT id, email_address, authorization_status, admin_user_id, device_type,
                   created_at, last_synch, daily_summaries_checkpoint, 
                   intraday_checkpoint, sleep_checkpoint
            FROM devices
            WHERE admin_user_id = %s
            ORDER BY created_at DESC
        """
        result = self.db.execute_query(query, (admin_user_id,))
        
        if result:
            return [
                Device(
                    id=row[0],
                    email_address=row[1],
                    authorization_status=row[2],
                    admin_user_id=row[3],
                    device_type=row[4],
                    created_at=row[5],
                    last_synch=row[6],
                    daily_summaries_checkpoint=row[7],
                    intraday_checkpoint=row[8],
                    sleep_checkpoint=row[9]
                )
                for row in result
            ]
        return []

    def get_all_authorized(self) -> List[Dict[str, Any]]:
        """
        Retrieve all authorized devices.

        Returns:
            List of dicts with id and email_address for authorized devices.
        """
        query = """
            SELECT id, email_address, authorization_status 
            FROM devices
        """
        result = self.db.execute_query(query)
        
        return [
            {'id': row[0], 'email_address': row[1]}
            for row in result if row[2] == 'authorized'
        ] if result else []

    def update_status(self, device_id: int, auth_status: str) -> bool:
        """
        Update the authorization status of a specific device.

        Valid statuses: 'inserted', 'authorized', 'non_active'

        Args:
            device_id: The primary key of the device to update.
            auth_status: The new authorization status.

        Returns:
            bool: True if update succeeded.

        Raises:
            AssertionError: If auth_status is not a permitted value.
        """
        assert auth_status in ['inserted', 'authorized', 'non_active'], \
            f"Invalid status: {auth_status}"

        query = """
            UPDATE devices
            SET authorization_status = %s
            WHERE id = %s
        """
        result = self.db.execute_query(query, (auth_status, device_id))
        
        if result:
            print(f"Status changed to {auth_status} for device {device_id}.")
        return bool(result)

    def update_device_type(self, device_id: int, device_type: str) -> bool:
        """
        Assign or update the device_type (source platform) of a device.

        Args:
            device_id: The device identifier.
            device_type: A descriptive type identifier.

        Returns:
            bool: True on success.
        """
        query = """
            UPDATE devices
            SET device_type = %s
            WHERE id = %s
        """
        result = self.db.execute_query(query, (device_type, device_id))
        return bool(result)

    def get_tokens(self, device_id: int) -> Tuple[Optional[str], Optional[str]]:
        """
        Fetch and decrypt stored access/refresh tokens.

        Args:
            device_id: The device identifier.

        Returns:
            Tuple of (access_token, refresh_token), both may be None
        """
        query = """
            SELECT access_token, refresh_token
            FROM devices
            WHERE id = %s
            ORDER BY created_at DESC, id DESC
            LIMIT 1
        """
        result = self.db.execute_query(query, (device_id,))
        
        if result:
            encrypted_access_token, encrypted_refresh_token = result[0]
            
            if encrypted_access_token and encrypted_refresh_token:
                access_token = decrypt_token(encrypted_access_token)
                refresh_token = decrypt_token(encrypted_refresh_token)
                return access_token, refresh_token
                
        return None, None

    def update_tokens(
        self, 
        device_id: int, 
        access_token: str, 
        refresh_token: str
    ) -> bool:
        """
        Encrypt and store new OAuth tokens for a device.

        Args:
            device_id: The device to update.
            access_token: New access token.
            refresh_token: New refresh token.

        Returns:
            bool: True on success.
        """
        encrypted_access_token = encrypt_token(access_token)
        encrypted_refresh_token = encrypt_token(refresh_token)

        query = """
            UPDATE devices
            SET access_token = %s, refresh_token = %s
            WHERE id = %s
        """
        result = self.db.execute_query(
            query, 
            (encrypted_access_token, encrypted_refresh_token, device_id)
        )
        return bool(result)

    def update_last_synch(self, device_id: int, timestamp: datetime) -> bool:
        """
        Save a new last-synch timestamp for a device.

        Args:
            device_id: The device identifier.
            timestamp: The new synchronization timestamp.

        Returns:
            bool: True if the update succeeded.
        """
        query = """
            UPDATE devices
            SET last_synch = %s
            WHERE id = %s
        """
        result = self.db.execute_query(query, (timestamp, device_id))
        
        if result:
            print(f"Last synch date {timestamp} for device_id {device_id} successfully updated.")
        return bool(result)

    def update_daily_summaries_checkpoint(self, device_id: int, date_value: date) -> bool:
        """
        Update the daily summary sync checkpoint.

        Args:
            device_id: The device identifier.
            date_value: The date up to which daily summaries are collected.

        Returns:
            bool: True on success.
        """
        query = """
            UPDATE devices
            SET daily_summaries_checkpoint = %s
            WHERE id = %s
        """
        result = self.db.execute_query(query, (date_value, device_id))
        
        if result:
            print(f"Daily summaries checkpoint {date_value} for device_id {device_id} successfully updated.")
        return bool(result)

    def update_intraday_checkpoint(self, device_id: int, timestamp: datetime) -> bool:
        """
        Update the intraday metrics checkpoint for a given device.

        Args:
            device_id: Device identifier.
            timestamp: Timestamp of the newest intraday data collected.

        Returns:
            bool: True on success.
        """
        query = """
            UPDATE devices
            SET intraday_checkpoint = %s
            WHERE id = %s
        """
        result = self.db.execute_query(query, (timestamp, device_id))
        
        if result:
            print(f"Intraday checkpoint {timestamp} for device_id {device_id} successfully updated.")
        return bool(result)

    def update_sleep_checkpoint(self, device_id: int, date_value: date) -> bool:
        """
        Update the checkpoint for sleep data collection.

        Args:
            device_id: The device identifier.
            date_value: The new sleep checkpoint date.

        Returns:
            bool: True on success.
        """
        query = """
            UPDATE devices
            SET sleep_checkpoint = %s
            WHERE id = %s
        """
        result = self.db.execute_query(query, (date_value, device_id))
        
        if result:
            print(f"Sleep checkpoint {date_value} for device_id {device_id} successfully updated.")
        return bool(result)

    def get_last_synch(self, device_id: int) -> Optional[datetime]:
        """
        Return the most recent successful sync timestamp for a device.

        Args:
            device_id: The device to check.

        Returns:
            datetime or None if unavailable.
        """
        query = """
            SELECT last_synch
            FROM devices
            WHERE id = %s
        """
        result = self.db.execute_query(query, (device_id,))
        return result[0][0] if result else None

    def get_daily_summary_checkpoint(self, device_id: int) -> Optional[date]:
        """
        Return the checkpoint date up to which daily summaries have been collected.

        Args:
            device_id: The corresponding device.

        Returns:
            date or None if none exists.
        """
        query = """
            SELECT daily_summaries_checkpoint
            FROM devices
            WHERE id = %s
        """
        result = self.db.execute_query(query, (device_id,))
        return result[0][0] if result else None

    def get_intraday_checkpoint(self, device_id: int) -> Optional[datetime]:
        """
        Return the checkpoint timestamp up to which intraday metrics have been collected.

        Args:
            device_id: The corresponding device.

        Returns:
            datetime or None.
        """
        query = """
            SELECT intraday_checkpoint
            FROM devices
            WHERE id = %s
        """
        result = self.db.execute_query(query, (device_id,))
        return result[0][0] if result else None

    def get_sleep_checkpoint(self, device_id: int) -> Optional[date]:
        """
        Return the checkpoint date up to which sleep data has been collected.

        Args:
            device_id: The corresponding device.

        Returns:
            date or None.
        """
        query = """
            SELECT sleep_checkpoint
            FROM devices
            WHERE id = %s
        """
        result = self.db.execute_query(query, (device_id,))
        return result[0][0] if result else None
