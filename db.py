import psycopg2
import random
import bcrypt

from encryption import encrypt_token, decrypt_token
from psycopg2 import sql
from datetime import datetime, timedelta, date
from typing import Any, Optional, Union, List, Tuple, Dict

from config import DB_CONFIG

class DatabaseManager:
    def __init__(self) -> None:
        """
        Initialize a DatabaseManager instance.

        This sets up the connection and cursor attributes as None. Use
        connect() before issuing any query to the database.
        """
        self.connection = None
        self.cursor = None

    def connect(self) -> bool:
        """
        Open a connection to the PostgreSQL database.

        Uses credentials from config.DB_CONFIG. On success,
        initializes a cursor for query execution.

        Returns:
            bool: True if connection succeeded, False otherwise.
        """
        try:

            self.connection = psycopg2.connect(
                host=DB_CONFIG["host"],
                database=DB_CONFIG["database"],
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"],
                port=DB_CONFIG["port"],
                # sslmode=DB_CONFIG["sslmode"]
            )
            self.cursor = self.connection.cursor()
            return True
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            return False

    def close(self) -> None:
        """
        Close the open database cursor and connection.

        Ensures cleanup of resources. Safe to call even if
        connection was never established.
        """
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
        except Exception as e:
            print(f"Error closing the connection to the database: {e}")
        finally:
            self.cursor = None
            self.connection = None

    def commit(self) -> None:
        """
        Commit the current database transaction.

        No-op if there is no active connection. Should be
        called after INSERT/UPDATE/DELETE operations.
        """
        if self.connection:
            self.connection.commit()

    def rollback(self) -> None:
        """
        Roll back the current transaction.

        Useful to undo the last operation that raised an error.
        """
        if self.connection:
            self.connection.rollback()

    def execute_query(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> Union[List[Tuple[Any, ...]], bool, None]:
        """
        Execute any SQL query with optional parameters.

        This method handles execution, commits on success, and
        returns fetched results if present.

        Args:
            query (str): A SQL query to execute.
            params (tuple | list): Parameter values for parametric queries.

        Returns:
            list | bool | None: Fetched rows for SELECT,
                                 True for successful DDL/DML,
                                 None on failure.
        """
        try:
            self.cursor.execute(query, params or ())
            if self.cursor.description:  # If the query returns results
                result = self.cursor.fetchall()
                self.commit()
                return result
            self.commit()
            return True
        except Exception as e:
            print(f"Error executing query: {e}")
            self.rollback()
            return None

    def execute_many(self, query: str, params_list: List[Tuple[Any, ...]]) -> bool:
        """
        Run the same query multiple times with batch parameters.

        Args:
            query (str): A SQL query with placeholders.
            params_list (list): A list of parameter tuples.

        Returns:
            bool: True if successful for all executions, False on any failure.
        """
        try:
            self.cursor.executemany(query, params_list)
            self.commit()
            return True
        except Exception as e:
            print(f"Error executing multiple queries: {e}")
            self.rollback()
            return False


    def verify_admin_user(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Authenticate an admin user.

        Checks the username and bcrypt-hashed password against the admin_users table.
        On success, updates last_login to the current timestamp.

        Args:
            username (str): The admin username.
            password (str): The plaintext password to verify.

        Returns:
            dict | None: A dict with user id, username, and full name on success,
                          None if credentials are invalid or user inactive.
        """

        query = """
            SELECT id, username, password_hash, full_name
            FROM admin_users
            WHERE username = %s AND is_active = TRUE
        """
        result = self.execute_query(query, (username,))
        
        if result:
            user_id, username, password_hash, full_name = result[0]
            # Verify password
            if bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8')):
                # Update last login
                self.execute_query("""
                    UPDATE admin_users 
                    SET last_login = CURRENT_TIMESTAMP 
                    WHERE id = %s
                """, (user_id,))
                return {
                    'id': user_id,
                    'username': username,
                    'full_name': full_name
                }
        return None


    def get_admin_user_by_id(self, admin_user_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch basic profile of a specific admin user.

        Args:
            admin_user_id (int): The ID of the admin user.

        Returns:
            dict | None: Profile fields, or None if no user found.
        """
        query = """
            SELECT username, full_name, created_at, last_login
            FROM admin_users
            WHERE id = %s
        """
        result = self.execute_query(query, (admin_user_id,))
        return {"username": result[0][0], "full_name": result[0][1], "created_at": result[0][2], "last_login": result[0][3]} if result else None


    def get_admin_user_devices(self, admin_user_id: int) -> Optional[List[Tuple[Any, ...]]]:
        """
        List all devices linked to a particular admin user.

        Args:
            admin_user_id (int): The admin user's primary key.

        Returns:
            list: A list of device rows sorted by creation date descending.
        """
        query = """
            SELECT id, email_address, authorization_status, device_type
            FROM devices
            WHERE admin_user_id = %s
            ORDER BY created_at DESC
        """
        return self.execute_query(query, (admin_user_id,))

    
    def update_admin_user_password(self, admin_user_id: int, new_password: str) -> Union[List[Tuple[Any, ...]], bool, None]:
        """
        Change the stored password of an admin user.

        Args:
            admin_user_id (int): The user to update.
            new_password (str): New plaintext password.

        Returns:
            bool: Result of password hash update query.
        """
        password_hash = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        query = """
            UPDATE admin_users
            SET password_hash = %s
            WHERE id = %s
        """

        return self.execute_query(query, (password_hash, admin_user_id))


    def get_all_admin_users(self) -> Optional[List[Tuple[Any, ...]]]:
        """
        Retrieve all users from admin_users.

        Returns:
            list | None: All user records ordered by creation date.
        """
        query = """
            SELECT id, username, email, full_name, created_at, last_login, is_active
            FROM admin_users
            ORDER BY created_at DESC
        """
        return self.execute_query(query)


    def add_device(
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
            admin_user_id (int): Owner admin user.
            email_address (str): Unique identifier for the device.
            access_token (str | None): Token returned from an external provider.
            refresh_token (str | None): Token used to refresh the access_token.

        Returns:
            int | None: The new device's id if successful, None otherwise.
        """
        if access_token and refresh_token:
            encrypted_access_token = encrypt_token(access_token)
            encrypted_refresh_token = encrypt_token(refresh_token)
        else:
            encrypted_access_token = None
            encrypted_refresh_token = None

        query = """
            INSERT INTO device (email_address, authorization_status, access_token, refresh_token, admin_user_id)
            VALUES (%s, 'inserted', %s, %s, %s)
            RETURNING id
        """
        result = self.execute_query(query, (email_address, encrypted_access_token, encrypted_refresh_token, admin_user_id))
        return result[0][0] if result else None


    def change_device_status(
        self, device_id: int, auth_status: str
    ) -> Union[List[Tuple[Any, ...]], bool, None]:
        """
        Update the authorization status of a specific device.

        This function sets the `authorization_status` column of a device
        to one of the predefined states. Only valid statuses are allowed:
        - 'inserted'   : Newly inserted device awaiting authorization
        - 'authorized' : Device has completed authorization
        - 'non_active' : Device has been marked inactive

        Args:
            device_id (int): The primary key of the device to update.
            auth_status (str): The new authorization status. Must be one of
                            'inserted', 'authorized', or 'non_active'.

        Returns:
            list | bool | None:
                - If the update succeeds, returns the result of the
                underlying SQL execution (often True or a result list).
                - If the database operation fails, returns None.

        Raises:
            AssertionError: If `auth_status` is not a permitted status value.

        Side Effects:
            - Updates the device’s row in the `devices` table.
            - Commits the transaction when successful; rolls back on error.
            - Logs a message to stdout indicating the result.

        Example:
            >>> db.change_device_status(42, 'authorized')
            True
        """

        assert auth_status in ['inserted', 'authorized', 'non_active']

        query = """
            UPDATE devices
            SET authorization_status = %s
            WHERE id = %s;
        """
        result = self.execute_query(query, (auth_status, device_id))
        if result:
            print(f"Status changed to {auth_status} for device {device_id}.")
        return result


    def get_daily_summaries(
        self,
        email_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[Tuple[Any, ...]]:

        """
        Retrieve daily summary records for a specific device/email within an optional date range.

        Daily summaries represent high-level aggregated metrics (e.g., steps,
        heart rate, calories) collected per day.

        Args:
            device_id (int): Identifier for the device whose summaries to fetch.
            start_date (datetime.date | datetime | None): Include summaries on/after this date.
            end_date (datetime.date | datetime | None): Include summaries on/before this date.

        Returns:
            list: A chronologically ordered list of summary tuples.
                  Returns an empty list if none are found.
        """

        params = [device_id]

        if start_date:
            query += " AND date >= %s"
            params.append(start_date.date())
        if end_date:
            query += " AND date <= %s"
            params.append(end_date.date())

        query += " ORDER BY date ASC"

        result = self.execute_query(query, params)
        return result if result else []

    def get_intraday_metrics(
        self,
        device_id: int,
        metric_type: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Optional[List[Tuple[datetime, float]]]:
        """
        Fetch intraday (timestamped) metric records for a specific device.

        Intraday metrics are time-series values such as heart rate, steps,
        calories, or distance captured throughout the day.

        Args:
            device_id (int): The device/email identifier.
            metric_type (str): Column name representing the metric (e.g., 'heart_rate').
            start_time (datetime | None): Only include records after this timestamp.
            end_time (datetime | None): Only include records before this timestamp.

        Returns:
            list: Time-ordered (time, value) tuples for the requested metric.
        """
        query = """
            SELECT time, value FROM intraday_metrics
            WHERE device_id = %s AND type = %s
        """
        params = [device_id, metric_type]

        if start_time:
            query += " AND time >= %s"
            params.append(start_time)

        if end_time:
            query += " AND time <= %s"
            params.append(end_time)

        query += " ORDER BY time"

        return self.execute_query(query, params)

    
    def get_last_synch(self, device_id: int) -> Optional[datetime]:
        """
        Return the most recent successful sync timestamp for a device.

        Args:
            device_id (int): The device to check.

        Returns:
            datetime | None: Last synchronization timestamp or None if unavailable.
        """

        query = """
            SELECT last_synch
            FROM devices
            WHERE id = %s
        """

        result = self.execute_query(query, (device_id,))
            
        if result:
            return result[0][0]
        return None


    def get_daily_summary_checkpoint(self, device_id: int) -> Optional[date]:
        """
        Return the checkpoint date up to which daily summaries have been collected.

        Useful for incremental sync logic.

        Args:
            device_id (int): The corresponding device.

        Returns:
            date | None: The last saved summary date, or None if none exists.
        """
        query = """
            SELECT daily_summaries_checkpoint
            FROM devices
            WHERE id = %s
        """
        result = self.execute_query(query, (device_id,))
            
        if result:
            return result[0][0]
        return None


    def get_intraday_checkpoint(self, device_id: int) -> Optional[datetime]:
        """
        Return the checkpoint timestamp up to which intraday metrics have been collected.

        Args:
            device_id (int): The corresponding device.

        Returns:
            datetime | None: Last intraday sync timestamp, or None.
        """
        query = """
            SELECT intraday_checkpoint
            FROM devices
            WHERE id = %s
        """
        result = self.execute_query(query, (device_id,))
            
        if result:
            return result[0][0]
        return None


    def get_sleep_checkpoint(self, device_id: int) -> Optional[date]:
        """
        Return the checkpoint date up to which sleep data has been collected.

        Args:
            device_id (int): The corresponding device.

        Returns:
            date | None: Last sleep summary date, or None.
        """
        query = """
            SELECT sleep_checkpoint
            FROM devices
            WHERE id = %s
        """
        result = self.execute_query(query, (device_id,))
            
        if result:
            return result[0][0]
        return None


    def update_last_synch(self, device_id: int, timestamp: datetime) -> Union[List[Tuple[Any, ...]], bool, None]:
        """
        Save a new last-synch timestamp for a device.

        Args:
            device_id (int): The device identifier.
            timestamp (datetime): The new synchronization timestamp.

        Returns:
            bool: True if the update succeeded.
        """

        query = """
                UPDATE devices
                SET last_synch = %s
                WHERE id = %s;
        """
        result = self.execute_query(query, (timestamp, device_id))
                
        if result:
            print(f"Last synch date {timestamp} for device_id {device_id} successfully updated.")
        return result

    
    def update_daily_summaries_checkpoint(self, device_id: int, date_value: date) -> Union[List[Tuple[Any, ...]], bool, None]:
        """
        Update the daily summary sync checkpoint.

        Args:
            device_id (int): The device identifier.
            date (datetime.date): The date up to which daily summaries are collected.

        Returns:
            bool: True on success.
        """

        query = """
                UPDATE devices
                SET daily_summaries_checkpoint = %s
                WHERE id = %s;
        """
        result = self.execute_query(query, (date, device_id))
                
        if result:
            print(f"Daily summaries chechpoint {date} for devices_id {device_id} successfully updated.")
        return result

    
    def update_intraday_checkpoint(self, email_id: int, timestamp: datetime) -> Union[List[Tuple[Any, ...]], bool, None]:
        """
        Update the intraday metrics checkpoint for a given device.

        Args:
            email_id (int): Email/device identifier.
            timestamp (datetime): Timestamp of the newest intraday data collected.

        Returns:
            bool: True on success.
        """

        query = """
                UPDATE email_addresses
                SET intraday_checkpoint = %s
                WHERE id = %s;
        """
        result = self.execute_query(query, (timestamp, email_id))
                
        if result:
            print(f"Intraday checkpoint {timestamp} for email_id {email_id} successfully updated.")
        return result


    def update_sleep_checkpoint(self, device_id: int, date_value: date) -> Union[List[Tuple[Any, ...]], bool, None]:
        """
        Update the checkpoint for sleep data collection.

        Args:
            device_id (int): The device identifier.
            date (datetime.date): The new sleep checkpoint date.

        Returns:
            bool: True on success.
        """

        query = """
                UPDATE devices
                SET sleep_checkpoint = %s
                WHERE id = %s;
        """
        result = self.execute_query(query, (date, device_id))
                
        if result:
            print(f"Sleep checkpoint {date} for device {device_id} with email address {email_address} successfully updated.")
        return result


    def get_sleep_logs(
        self,
        email_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Optional[List[Tuple[Any, ...]]]:

        """
        Fetch sleep log entries for a device across an optional date range.

        Args:
            email_id (int): Device/email identifier.
            start_date (datetime | None): Only include logs after this time.
            end_date (datetime | None): Only include logs before this time.

        Returns:
            list: Sleep log tuples ordered by start time descending.
        """
        query = """
            SELECT * FROM sleep_logs
            WHERE email_id = %s
        """
        params = [email_id]

        if start_date:
            query += " AND start_time >= %s"
            params.append(start_date)

        if end_date:
            query += " AND start_time <= %s"
            params.append(end_date)

        query += " ORDER BY start_time DESC"

        return self.execute_query(query, tuple(params))


    def get_user_alerts(
        self,
        email_id: int,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        acknowledged: Optional[bool] = None,
    ) -> Optional[List[Tuple[Any, ...]]]:
        """
        Retrieve alert events for a device.

        Alerts may indicate threshold violations or other conditions.

        Args:
            email_id (int): Device/email identifier.
            start_time (datetime | None): Only include alerts after this.
            end_time (datetime | None): Only include alerts before this.
            acknowledged (bool | None): Only include acknowledged (True),
                                        unacknowledged (False), or all (None).

        Returns:
            list: Alerts ordered by most recent first.
        """
        query = """
            SELECT * FROM alerts
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

        return self.execute_query(query, params)


    def insert_alert(
        self,
        email_id: int,
        alert_type: str,
        priority: str,
        triggering_value: float,
        threshold: Union[str, float],
        timestamp: Optional[datetime] = None,
        details: Optional[str] = None
    ) -> Optional[int]:
        """
        Create a new alert record.

        Args:
            email_id (int): Device/email identifier.
            alert_type (str): A descriptive alert category.
            priority (str): One of 'high', 'medium', or 'low'.
            triggering_value (float): The value that triggered the alert condition.
            threshold (str | float | tuple): The threshold definition.
            timestamp (datetime | None): When the alert occurred.
            details (str | None): Additional context.

        Returns:
            int | None: The new alert’s ID on success.
        """
        try:
            if timestamp is None:
                timestamp = datetime.now()

            # Convert threshold to a string if it isn’t already.
            threshold = str(threshold)

            query = """
                INSERT INTO alerts (
                    email_id, alert_type, priority, triggering_value, threshold_value, alert_time, details
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            result = self.execute_query(query, (email_id, alert_type, priority, triggering_value, threshold, timestamp, details))
            return result[0][0] if result else None
        except Exception as e:
            print(f"Error executing query: {e}")
            return None


    def get_alert_by_id(self, alert_id: int) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific alert and associated user details.

        Args:
            alert_id (int): The alert’s primary key.

        Returns:
            dict | None: Alert fields + user name/email if found.
        """
        if not self.connect():
            return None

        try:
            query = """
                SELECT a.*, u.name as user_name, u.email as user_email
                FROM alerts a
                JOIN users u ON a.user_id = u.id
                WHERE a.id = %s
            """
            result = self.execute_query(query, [alert_id])
            if result and len(result) > 0:
                # Convert the result to a dictionary
                columns = [desc[0] for desc in self.cursor.description]
                alert = dict(zip(columns, result[0]))
                return alert
            return None
        except Exception as e:
            print(f"Error al obtener alerta por ID: {str(e)}")
            return None

    def store_pending_auth(self, device_id: int, state: str, code_verifier: str) -> Union[List[Tuple[Any, ...]], bool, None]:
        """
        Save a new authorization attempt awaiting completion.

        Args:
            device_id (int): The device requesting OAuth/consent.
            state (str): A unique state value for callback correlation.
            code_verifier (str): Challenge for PKCE flow.

        Returns:
            bool: True on success.
        """
        query = """
            INSERT INTO pending_authorizations (device_id, state, code_verifier, expires_at)
            VALUES (%s, %s, %s, NOW() + INTERVAL '10 minutes')
        """
        return self.execute_query(query, (device_id, state, code_verifier))

    def get_pending_auth(self, state: str) -> Optional[Dict[str, Any]]:
        """
        Fetch an unexpired pending authorization by state.

        Args:
            state (str): The PKCE callback state.

        Returns:
            dict | None: Contains 'code_verifier' and 'device_id'.
        """
        query = """
            SELECT code_verifier, device_id
            FROM pending_authorizations
            WHERE state = %s AND expires_at > NOW()
        """
        result = self.execute_query(query, (state,))
        if result:
            return {'code_verifier': result[0][0], 'device_id': result[0][1]}
        return None

    def check_pending_auth(self, device_id: int) -> bool:
        """
        Check existence of an unexpired pending authorization for a device.

        Args:
            device_id (int): Device with pending authorization.

        Returns:
            bool: True if a pending auth exists.
        """
        query = """
            SELECT *
            FROM pending_authorizations
            WHERE device_id = %s AND expires_at > NOW()
        """
        result = self.execute_query(query, (device_id,))
        if result:
            return True
        return False

    def delete_pending_auth(self, state: str) -> Union[List[Tuple[Any, ...]], bool, None]:
        """
        Remove a pending authorization once used or expired.

        Args:
            state (str): The pending auth state value.

        Returns:
            bool: True on success.
        """
        query = "DELETE FROM pending_authorizations WHERE state = %s"
        return self.execute_query(query, (state,))

    def get_device_by_email_address(self, email_address: str) -> Optional[int]:
        """
        Find the latest device record associated with an email.

        Args:
            email_address (str): The address identifier.

        Returns:
            int | None: The device’s ID if found.
        """
        query = """
            SELECT id FROM devices
            WHERE email_address = %s
            ORDER BY created_at DESC
            LIMIT 1;
        """
        result = self.execute_query(query, (email_address,))
        return result[0][0] if result else None

    def get_device_tokens(self, device_id: int) -> Tuple[Optional[str], Optional[str]]:
        """
        Fetch and decrypt stored access/refresh tokens.

        Args:
            device_id (int): The device identifier.

        Returns:
            tuple[str | None, str | None]: (access_token, refresh_token)
        """
        query = """
            SELECT access_token, refresh_token
            FROM devices
            WHERE id = %s
            ORDER BY created_at DESC, id DESC
            LIMIT 1;
        """
        result = self.execute_query(query, (device_id,))
        if result:
            encrypted_access_token, encrypted_refresh_token = result[0]
            # Decrypt the tokens
            access_token = decrypt_token(encrypted_access_token)
            refresh_token = decrypt_token(encrypted_refresh_token)
            return access_token, refresh_token
        return None, None


    def update_device_tokens(self, device_id: int, access_token: str, refresh_token: str) -> Union[List[Tuple[Any, ...]], bool, None]:
        """
        Encrypt and store new OAuth tokens for a device.

        Args:
            device_id (int): The device to update.
            access_token (str): New access token.
            refresh_token (str): New refresh token.

        Returns:
            bool: True on success.
        """

        # Encrypt the tokens before storing them
        encrypted_access_token = encrypt_token(access_token)
        encrypted_refresh_token = encrypt_token(refresh_token)

        query = """
            UPDATE devices
            SET access_token = %s, refresh_token = %s
            WHERE id = %s;
        """
        result = self.execute_query(query, (encrypted_access_token, encrypted_refresh_token, device_id))
        return result


    def update_device_type(self, device_id: int, device_type: str) -> Union[List[Tuple[Any, ...]], bool, None]:
        """
        Assign or update the device_type (source platform) of a device.

        Args:
            device_id (int): The device identifier.
            device_type (str): A descriptive type identifier.

        Returns:
            bool: True on success.
        """

        query = """
            UPDATE devices
            SET device_type = %s
            WHERE id = %s;
        """
        result = self.execute_query(query, (device_type, device_id))
        return result


    def check_intraday_timestamp(self, device_id: int, timestamp: datetime) -> bool:
        """
        Determine if a given timestamp already exists for intraday data.

        Args:
            device_id (int): The device to check.
            timestamp (datetime): The specific timestamp.

        Returns:
            bool: True if exists.
        """
        query = """
            SELECT * FROM intraday_metrics
            WHERE device_id = %s
            AND time = %s
        """
        result = self.execute_query(query, (device_id, timestamp))
        return bool(result)


    def insert_intraday_metric(
        self,
        device_id: int,
        timestamp: datetime,
        data_type: str = "heart_rate",
        value: Optional[float] = None,
    ) -> Union[List[Tuple[Any, ...]], bool, None]:
        """
        Save or update a metric value at a particular timestamp.

        If a row exists, updates just the given column; otherwise
        inserts a new record with other fields NULL.

        Args:
            device_id (int): Device identifier.
            timestamp (datetime): Exact capture time.
            data_type (str): Which intraday column (e.g., 'steps').
            value (float): The measured value.

        Returns:
            bool: True on success.
        """
        if self.check_intraday_timestamp(device_id, timestamp):
            # Update existing record
            query = f"""
                UPDATE intraday_metrics
                SET {data_type} = %s
                WHERE device_id = %s
                AND time = %s
            """
            result = self.execute_query(query, (value, device_id, timestamp))
            if result:
                print(f"Intraday {data_type} data for device {device_id} successfully updated in intraday_metrics.")
            return result
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
                VALUES (%s, %s, %s, %s, %s, %s);
            """
            result = self.execute_query(query, (device_id, timestamp, values["heart_rate"], values["steps"], values["calories"], values["distance"]))
            if result:
                print(f"Intraday {data_type} data for device {device_id} successfully saved in intraday_metrics.")
            return result


    def insert_sleep_session(self, device_id: int) -> Optional[int]:
        """
        Create a new sleep session grouping for inserting logs.

        Args:
            device_id (int): Which device the sleep belongs to.

        Returns:
            int | None: The new sleep session ID.
        """
        query = """
            INSERT INTO sleep_sessions (device_id) 
            VALUES (%s)
            RETURNING id;
        """

        result = self.execute_query(query, (device_id, ))
        if result:
            print(f"Sleep session inserted for device {device_id}")
        return result[0][0]


    def insert_sleep_log(self, sleep_session_id: int, data: Dict[str, Any]) -> Union[List[Tuple[Any, ...]], bool, None]:
        """
        Persist a detailed sleep segment.

        Args:
            sleep_session_id (int): The parent sleep session.
            data (dict): Sleep fields from an external API.

        Returns:
            bool: True on success.
        """

        query = """
            INSERT INTO sleep_logs (
                sleep_session_id, start_time, end_time, is_main_sleep, duration, 
                minutes_asleep, minutes_awake, minutes_in_the_bed, log_type, type
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        result = self.execute_query(query, (
            sleep_session_id,
            data['startTime'], 
            data['endTime'],
            data['isMainSleep'],
            data['duration'] / 1000,
            data['minutesAsleep'],
            data['minutesAwake'],
            data['timeInBed'],
            data['logType'],
            data['type']
        ))

        if result:
            print(f"Sleep record inserted for sleep session {sleep_session_id}")
        return result


    def insert_sleep_level(self, sleep_session_id: int, data: Dict[str, Any]) -> Union[List[Tuple[Any, ...]], bool, None]:
        """
        Insert a detailed sleep level entry (e.g., REM, deep, awake).

        Args:
            sleep_session_id (int): Parent session.
            data (dict): Level info.

        Returns:
            bool: True on success.
        """
        query = """
            INSERT INTO sleep_levels (
                sleep_session_id, time, level, seconds
            ) 
            VALUES (%s, %s, %s, %s)
        """

        result = self.execute_query(query, (
            sleep_session_id, 
            data['dateTime'], 
            data['level'],
            data['seconds'],
        ))

        if result:
            print(f"Sleep level record inserted for sleep session {sleep_session_id}")
        return result

    
    def insert_sleep_short_level(self, sleep_session_id: int, short: Dict[str, Any]) -> Union[List[Tuple[Any, ...]], bool, None]:
        """
        Persist a “short level” segment within a sleep session.

        Args:
            sleep_session_id (int): Parent session.
            short (dict): Entry data.

        Returns:
            bool: True on success.
        """
        query = """
            INSERT INTO sleep_short_levels (
                sleep_session_id, time, seconds
            ) 
            VALUES (%s, %s, %s)
        """

        result = self.execute_query(query, (
            sleep_session_id, 
            short['dateTime'],
            short['seconds'],
        ))

        if result:
            print(f"Sleep short level record inserted for sleep_log {sleep_session_id}")
        return result


    def get_device_history(self, device_id: int) -> List[Tuple[Any, ...]]:
        """
        Return full history of daily summaries for a device.

        Args:
            device_id (int): The device identifier.

        Returns:
            list: All daily summary rows ordered by date.
        """
        query = """
            SELECT * FROM daily_summaries
            WHERE device_id = %s
            ORDER BY date;
        """
        result = self.execute_query(query, (device_id,))
        return result if result else []


    def insert_daily_summary(self, device_id, date, **data):
        """Inserts or updates a daily summary in the daily_summaries table"""
        query = """
            INSERT INTO daily_summaries (
                device_id, date, steps, heart_rate, sleep_minutes,
                calories, distance, floors, elevation, active_minutes,
                sedentary_minutes, nutrition_calories, water, weight,
                bmi, fat, oxygen_saturation, respiratory_rate, temperature
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (email_id, date) DO UPDATE SET
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
                temperature = EXCLUDED.temperature;
        """
        result = self.execute_query(query, (
            email_id, date,
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
        return result


    def get_all_devices(self):
        """Retrieves a list of unique devices from the database"""

        query = "SELECT id, email_address, authorization_status FROM devices;"
        result = self.execute_query(query)
        return [{
                    'id': row[0], 
                    'email_address': row[1]
                } for row in result if row[2] == 'authorized'] if result else []


    def get_intraday_data_timestamps_by_range(self, device_id, start_date, end_date):

        query = "SELECT time FROM intraday_metrics WHERE device_id = %s AND time > %s AND time < %s ORDER BY TIME;"
        result = self.execute_query(query, (device_id, start_date, end_date))
        return result if result else []

def connect_to_db():
    """
    Function to maintain compatibility with existing code.
    DEPRECATED: Use DatabaseManager class instead for better connection management.
    """
    import warnings
    warnings.warn("connect_to_db() is deprecated. Use DatabaseManager class instead.", DeprecationWarning, stacklevel=2)
    
    try:
        connection = psycopg2.connect(
            host=DB_CONFIG["host"],
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            port=DB_CONFIG["port"],
            # sslmode=DB_CONFIG["sslmode"]
        )
        return connection
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def init_db():
    """
        Initializes the database by creating tables if they do not exist and configuring TimeScaleDB.
    """

    db = DatabaseManager()
    if not db.connect():
        print("Failed to connect to the database")
        return False

    try:
        # Check if TimeScaleDB is installed
        result = db.execute_query("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';")
        if not result:
            print("TimeScaleDB is not installed. Please install the extension first.")
            print("Visit the following link: https://docs.timescale.com/install/latest/self-hosted/windows/installation/")
            return False

        # Enable TimeScaleDB extension
        db.execute_query("CREATE EXTENSION IF NOT EXISTS timescaledb;")

        db.execute_query("""
            CREATE TABLE IF NOT EXISTS admin_users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                full_name VARCHAR(255),
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMPTZ,
                is_active BOOLEAN DEFAULT TRUE
            );
        """)

        db.execute_query("CREATE TYPE status_type AS ENUM ('inserted', 'authorized', 'non_active');")

        # Create email_addresses table
        db.execute_query("""
            CREATE TABLE IF NOT EXISTS email_addresses (
                id SERIAL PRIMARY KEY,
                address_name VARCHAR(255) NOT NULL,
                device_type VARCHAR(50),
                status status_type NOT NULL DEFAULT 'inserted',
                admin_user_id INTEGER REFERENCES admin_users(id),
                access_token TEXT,
                refresh_token TEXT,
                daily_summaries_checkpoint DATE,
                intraday_checkpoint TIMESTAMPTZ,
                last_synch TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)

        db.execute_query("""
            CREATE TABLE pending_authorizations (
                    id SERIAL PRIMARY KEY,
                    email_id INTEGER REFERENCES email_addresses(id),
                    state VARCHAR(500) UNIQUE NOT NULL,
                    code_verifier VARCHAR(128) NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                );

                CREATE INDEX idx_pending_auth_state ON pending_authorizations(state);
                CREATE INDEX idx_pending_auth_expires ON pending_authorizations(expires_at);
        """)

        # Create daily summaries table
        db.execute_query("""
            CREATE TABLE IF NOT EXISTS daily_summaries (
                id SERIAL,
                device_id INTEGER REFERENCES email_addresses(id),
                date DATE NOT NULL,
                steps INTEGER,
                heart_rate INTEGER,
                sleep_minutes INTEGER,
                calories INTEGER,
                distance FLOAT,
                floors INTEGER,
                elevation FLOAT,
                active_minutes INTEGER,
                sedentary_minutes INTEGER,
                nutrition_calories INTEGER,
                water FLOAT,
                weight FLOAT,
                bmi FLOAT,
                fat FLOAT,
                oxygen_saturation FLOAT,
                respiratory_rate FLOAT,
                temperature FLOAT,
                UNIQUE(email_id, date)
            );
        """)

        # Convert it to hypertable
        db.execute_query("""
            SELECT create_hypertable('daily_summaries', 'date',
                if_not_exists => TRUE,
                migrate_data => TRUE
            );
        """)

        # Create intraday metrics table
        db.execute_query("""
            CREATE TABLE IF NOT EXISTS intraday_metrics (
                id SERIAL,
                device_id INTEGER REFERENCES email_addresses(id),
                time TIMESTAMPTZ,
                heart_rate FLOAT,
                steps FLOAT,
                calories FLOAT,
                distance FLOAT
            );
        """)


        # Convert it to hypertable
        db.execute_query("""
            SELECT create_hypertable('intraday_metrics', 'time',
                if_not_exists => TRUE,
                migrate_data => TRUE
            );
        """)

        db.execute_query("""
            CREATE TABLE sleep_sessions (
                id SERIAL PRIMARY KEY,
                device_id INTEGER REFERENCES email_addresses(id)
            );
        """)


        # Crear sleep log table
        db.execute_query("""
            CREATE TABLE sleep_logs (
                sleep_sessions_id INTEGER REFERENCES sleep_sessions(id),
                start_time TIMESTAMPTZ,
                end_time TIMESTAMPTZ,
                is_main_sleep BOOLEAN,
                duration INTEGER,
                minutes_asleep INTEGER,
                minutes_awake INTEGER,
                minutes_in_the_bed INTEGER,
                log_type VARCHAR(50),
                type VARCHAR(50)
            );
        """)

        # Convert it to hypertable
        db.execute_query("""
            SELECT create_hypertable('sleep_logs', 'start_time',
                if_not_exists => TRUE,
                migrate_data => TRUE
            );
        """)

        db.execute_query("""
            CREATE TABLE IF NOT EXISTS sleep_levels (
                id SERIAL,
                sleep_session_id INTEGER REFERENCES sleep_sessions(id),
                time TIMESTAMPTZ NOT NULL,
                level VARCHAR(50),
                seconds INTEGER
            );
        """)

        # Convert it to hypertable
        db.execute_query("""
            SELECT create_hypertable('sleep_levels', 'time',
                if_not_exists => TRUE,
                migrate_data => TRUE
            );
        """)


        db.execute_query("""
            CREATE TABLE IF NOT EXISTS sleep_short_levels (
                id SERIAL,
                sleep_session_id INTEGER REFERENCES sleep_sessions(id),
                time TIMESTAMPTZ NOT NULL,
                seconds INTEGER
            );
        """)

        # Convert it to hypertable
        db.execute_query("""
            SELECT create_hypertable('sleep_short_levels', 'time',
                if_not_exists => TRUE,
                migrate_data => TRUE
            );
        """)

        print("Database successfully initialized with TimeScaleDB.")
        return True

    except Exception as e:
        print(f"Error initializing the database: {e}")
        return False
    finally:
        db.close()


def save_to_db(user_id, date, **data):
    """
        Saves Fitbit data in the database using the new TimeScaleDB schema.
        DEPRECATED: Use DatabaseManager.insert_daily_summary() instead.

        Args:
            user_id (int): User ID.
            date (str): Date of the data (YYYY-MM-DD).
            data (dict): Dictionary with Fitbit data.
    """
    db = DatabaseManager()
    if not db.connect():
        print("Failed to connect to the database")
        return False
    
    try:
        result = db.insert_daily_summary(user_id, date, **data)
        if result:
            print(f"Data of user {user_id} successfully saved to daily_summaries.")
        return result
    except Exception as e:
        print(f"Error saving data: {e}")
        return False
    finally:
        db.close()



def run_tests():
    """
        Runs insertion and query tests to verify database functionality.
        Includes test cases for:
        - Normal data
        - Dropouts that trigger alerts
        - Erroneous or missing data
        - Inconsistent data
    """

    print("\n=== Starting tests with simulated data ===\n")

    # Case 1: Device with initial normal data
    print("1. Creating user with normal data...")
    user_id_1 = add_device(
        name="Device di prova",
        email="devicediprova@example.com",
        access_token="access_token",
        refresh_token="refresh_token"
    )

    # Insert 5 days of normal data
    from datetime import datetime, timedelta
    base_date = datetime.now().date()

    print("\n2. Inserting normal data for the first 5 days...")
    for i in range(5):
        date = (base_date - timedelta(days=i)).strftime("%Y-%m-%d")
        save_to_db(
            user_id=user_id_1,
            date=date,
            steps=10000,
            heart_rate=75,
            sleep_minutes=420,
            calories=2000,
            distance=8.5,
            floors=10,
            elevation=100.5,
            active_minutes=60,
            sedentary_minutes=480,
            nutrition_calories=1800,
            water=2.5,
            weight=70.5,
            bmi=22.5,
            fat=18.5,
            oxygen_saturation=98.0,
            respiratory_rate=16.5,
            temperature=36.5
        )

    # Case 2: Significant drop in physical activity
    print("\n3. Simulating a drop in physical activity...")
    date = (base_date + timedelta(days=1)).strftime("%Y-%m-%d")
    save_to_db(
        user_id=user_id_1,
        date=date,
        steps=2000,  # Significant drop in steps (80% less)
        heart_rate=90,  # Increase in heart rate
        sleep_minutes=420,
        calories=1200,
        distance=1.5,
        floors=2,
        elevation=20.5,
        active_minutes=15,  # Significant reduction in active minutes
        sedentary_minutes=900,  # Significant increase in sedentary time
        nutrition_calories=1800,
        water=2.0,
        weight=70.5,
        bmi=22.5,
        fat=18.5,
        oxygen_saturation=95.0,
        respiratory_rate=16.5,
        temperature=36.5
    )

    # Case 3: Erroneous or missing data
    print("\n4. Inserting data with errors and missing values...")
    date = (base_date + timedelta(days=2)).strftime("%Y-%m-%d")
    save_to_db(
        user_id=user_id_1,
        date=date,
        steps=None,  # Missing step data
        heart_rate=None,  # Missing heart rate data
        sleep_minutes=None,  # Missing sleep data
        calories=None,
        distance=None,
        floors=None,
        elevation=None,
        active_minutes=None,
        sedentary_minutes=None,
        nutrition_calories=None,
        water=None,
        weight=None,
        bmi=None,
        fat=None,
        oxygen_saturation=None,
        respiratory_rate=None,
        temperature=None
    )

    # Case 4: Inconsistent data
    print("\n5. Inserting inconsistent data...")
    date = (base_date + timedelta(days=3)).strftime("%Y-%m-%d")
    save_to_db(
        user_id=user_id_1,
        date=date,
        steps=15000,  # High number of steps
        heart_rate=95,  # Elevated heart rate
        sleep_minutes=480,
        calories=1200,  # Low calories for the activity
        distance=2.0,   # Low distance for the steps
        floors=25,      # High number of steps
        elevation=250.5,
        active_minutes=30,  # Low active minutes for the steps
        sedentary_minutes=600,
        nutrition_calories=3500,  # Very high nutrition calories
        water=1.0,
        weight=70.5,
        bmi=22.5,
        fat=18.5,
        oxygen_saturation=92.0,  # Slightly low oxygen saturation
        respiratory_rate=16.5,
        temperature=36.5
    )

    print("\n6. Evaluating alerts for the user...")
    from alert_rules import evaluate_all_alerts
    alerts = evaluate_all_alerts(user_id_1, datetime.now())
    print(f"Alerts generated: {alerts}")

    print("\n=== Tests completed ===\n")


def reset_database():
    """
    Resets the database by dropping all tables and recreating them.
    """
    connection = connect_to_db()
    if connection:
        try:
            with connection.cursor() as cursor:
                # Drop all tables in the correct order to handle foreign key constraints
                cursor.execute("DROP TABLE IF EXISTS alerts CASCADE;")  # Drop alerts first
                cursor.execute("DROP TABLE IF EXISTS sleep_logs CASCADE;")
                cursor.execute("DROP TABLE IF EXISTS intraday_metrics CASCADE;")
                cursor.execute("DROP TABLE IF EXISTS daily_summaries CASCADE;")
                cursor.execute("DROP TABLE IF EXISTS email_addresses CASCADE;")
                cursor.execute("DROP TABLE IF EXISTS admin_users CASCADE;")
                cursor.execute("DROP TABLE IF EXISTS pending_authorizations CASCADE;")

                connection.commit()
                print("Database tables dropped successfully.")

                # Reinitialize the database
                init_db()
                # print("Database reinitialized successfully.")

                db = DatabaseManager()
                if db.connect():
                    print("\nEnter new admin user details:")
                    password = input("Password: ").strip()
                    password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

                    result = db.execute_query("""
                                INSERT INTO admin_users (username, password_hash, full_name)
                                VALUES (%s, %s, %s)
                                RETURNING id
                            """, ('admin', password_hash, 'UNIMORE Administrator'))

        except Exception as e:
            print(f"Error resetting database: {e}")
            connection.rollback()
        finally:
            connection.close()

# def create_test_data():
#     """Creates test data for development"""
#     conn = connect_to_db()
#     if not conn:
#         return False

#     try:
#         with conn.cursor() as cursor:
#             cursor.execute("""
#                 INSERT INTO users (name, email, access_token, refresh_token)
#                 VALUES ('Test User', 'test@example.com', 'test_token', 'test_refresh')
#                 RETURNING id
#             """)
#             user_id = cursor.fetchone()[0]

#             # Alert date (today)
#             alert_date = datetime.now().date()

#             # Create activity data for the last 7 days
#             for i in range(7):
#                 date = alert_date - timedelta(days=i)
#                 cursor.execute("""
#                     INSERT INTO daily_summaries (
#                         user_id, date, steps, heart_rate, sleep_minutes,
#                         calories, distance, floors, elevation, active_minutes,
#                         sedentary_minutes
#                     ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#                     ON CONFLICT (user_id, date) DO UPDATE SET
#                         steps = EXCLUDED.steps,
#                         heart_rate = EXCLUDED.heart_rate,
#                         sleep_minutes = EXCLUDED.sleep_minutes,
#                         calories = EXCLUDED.calories,
#                         distance = EXCLUDED.distance,
#                         floors = EXCLUDED.floors,
#                         elevation = EXCLUDED.elevation,
#                         active_minutes = EXCLUDED.active_minutes,
#                         sedentary_minutes = EXCLUDED.sedentary_minutes
#                 """, (
#                     user_id, date,
#                     8000 + random.randint(-500, 500),  # steps
#                     70 + random.randint(-5, 5),        # heart_rate
#                     420 + random.randint(-30, 30),     # sleep_minutes
#                     2000 + random.randint(-200, 200),  # calories
#                     5.5 + random.uniform(-0.5, 0.5),   # distance
#                     10 + random.randint(-2, 2),        # floors
#                     100 + random.randint(-10, 10),     # elevation
#                     45 + random.randint(-10, 10),      # active_minutes
#                     600 + random.randint(-30, 30)      # sedentary_minutes
#                 ))

#                 # Create intraday data ONLY for the alert day (today)
#                 if date == alert_date:
#                     for hour in range(24):
#                         # Steps every hour
#                         time = datetime.combine(date, datetime.min.time()) + timedelta(hours=hour)
#                         steps = random.randint(0, 1000)
#                         cursor.execute("""
#                             INSERT INTO intraday_metrics (user_id, time, type, value)
#                             VALUES (%s, %s, %s, %s)
#                         """, (user_id, time, 'steps', steps))
#                         # Heart rate every hour
#                         hr = random.randint(60, 120)
#                         cursor.execute("""
#                             INSERT INTO intraday_metrics (user_id, time, type, value)
#                             VALUES (%s, %s, %s, %s)
#                         """, (user_id, time, 'heart_rate', hr))
#                         # Calories every hour
#                         calories = random.randint(50, 200)
#                         cursor.execute("""
#                             INSERT INTO intraday_metrics (user_id, time, type, value)
#                             VALUES (%s, %s, %s, %s)
#                         """, (user_id, time, 'calories', calories))

#             # Create test alerts for today's date
#             alert_types = [
#                 ('activity_drop', 'Low activity level detected'),
#                 ('sedentary_increase', 'Significant increase in sedentary time'),
#                 ('sleep_duration_change', 'Significant change in sleep duration'),
#                 ('heart_rate_anomaly', 'Anomaly detected in heart rate')
#             ]
#             for i in range(3):
#                 alert_time = datetime.combine(alert_date, datetime.min.time()) + timedelta(hours=8*i)
#                 alert_type, message = random.choice(alert_types)
#                 cursor.execute("""
#                     INSERT INTO alerts (
#                         user_id, alert_time, alert_type, priority, details
#                     ) VALUES (%s, %s, %s, %s, %s)
#                 """, (
#                     user_id, alert_time, alert_type,
#                     random.choice(['low', 'medium', 'high']),
#                     message
#                 ))
#             # Unacknowledged high-priority alert for today
#             cursor.execute("""
#                 INSERT INTO alerts (
#                     user_id, alert_time, alert_type, priority, details, acknowledged
#                 ) VALUES (%s, %s, %s, %s, %s, %s)
#             """, (
#                 user_id, datetime.combine(alert_date, datetime.min.time()) + timedelta(hours=17),
#                 'heart_rate_anomaly', 'high',
#                 'Abnormally high heart rate',
#                 False
#             ))
#             conn.commit()
#             print("Test data created successfully")
#             return True
#     except Exception as e:
#         conn.rollback()
#         print(f"Test data created successfully: {str(e)}")
#         return False
#     finally:
#         conn.close()

def drop_intraday_data():
    """Drops intraday data for device_id=3"""
    db = DatabaseManager()
    if not db.connect():
        print("Failed to connect to the database")
        return False

    try:
        print(f"Dropping intraday table")
        query = "DELETE FROM intraday_metrics WHERE device_id=3;"
        result = db.execute_query(query, [])
        return result
    except Exception as e:
        print(f"Error while dropping intraday table: {e}")
        return False
    finally:
            db.close()


def reset_device_status():
    """Deletes access tokens from all email addresses"""
    db = DatabaseManager()
    if not db.connect():
        print("Failed to connect to the database")
        return False
    
    try:
        print(f"Resetting status")
        query = "UPDATE devices SET access_token = NULL, refresh_token = NULL, device_type = NULL, authorization_status='inserted';"
        result = db.execute_query(query, [])

        return result
    except Exception as e:
        print(f"Error while dropping access tokens: {e}")
        return False
    finally:
        db.close()


def drop_authorizations():
    """Drops pending authorizations table"""
    db = DatabaseManager()
    if not db.connect():
        print("Failed to connect to the database")
        return False

    try:
        print(f"Dropping authorizations")
        query = "DROP TABLE IF EXISTS pending_authorizations CASCADE;"
        result = db.execute_query(query, [])
        return result
    except Exception as e:
        print(f"Error dropping authorizations: {e}")
        return False
    finally:
        db.close()


def drop_fitbit_data():

    """Drops all fitbit data table"""
    db = DatabaseManager()
    if not db.connect():
        print("Failed to connect to the database")
        return False

    try:
        print(f"Dropping all fitbit data")
        query = "DELETE FROM daily_summaries;"
        result = db.execute_query(query, [])

        return result
    except Exception as e:
        print(f"Error dropping all fitbit data: {e}")
        return False
    finally:
        db.close()


if __name__ == "__main__":
    # drop_authorizations()
    # reset_database()

    # Create test data
    # reset_emails_status()
    # drop_fitbit_data()

    db = DatabaseManager()
    if not db.connect():
       print("Failed to connect to the database")
    else:

        try:

            reset_device_status()

            from auth import refresh_tokens
            for device_id in [1, 2]:

                access_token, refresh_token = db.get_device_tokens(device_id)

                new_access_token, new_refresh_token = refresh_tokens(refresh_token)

                db.update_device_tokens(device_id, new_access_token, new_refresh_token)

            # print("Dati aggiornati!")

            # query = "UPDATE email_addresses SET daily_summaries_checkpoint = '2026-01-20' WHERE id=1;"

            # query = "UPDATE email_addresses SET access_token = NULL, refresh_token = NULL, status='inserted' WHERE 1=1;"
            # result = db.execute_query(query, [])

        finally:
            db.close()


# You see that right now the authentication reserved to a single admin account whose credential are in environment variables. I want to change this by including the possibility more users, each of wich handles its own email addresses. What the best way to do it?