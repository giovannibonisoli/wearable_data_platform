import psycopg2
from psycopg2 import sql
from config import DB_CONFIG
from encryption import encrypt_token, decrypt_token
import random
from datetime import datetime, timedelta
import bcrypt

class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self):
        """Establish the connection with the database."""
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

    def close(self):
        """Close the connection with the database."""
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

    def commit(self):
        """Commit the current transaction."""
        if self.connection:
            self.connection.commit()

    def rollback(self):
        """Rollback the current transaction."""
        if self.connection:
            self.connection.rollback()

    def execute_query(self, query, params=None):
        """Execute a query and return the results."""
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

    def execute_many(self, query, params_list):
        """Execute a query multiple times with different parameters."""
        try:
            self.cursor.executemany(query, params_list)
            self.commit()
            return True
        except Exception as e:
            print(f"Error executing multiple queries: {e}")
            self.rollback()
            return False


    def verify_admin_user(self, username, password):
        """Verify admin user credentials"""

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


    def get_admin_user_by_id(self, admin_user_id):
        """Get all email addresses owned by an admin user"""
        query = """
            SELECT username, full_name, created_at, last_login
            FROM admin_users
            WHERE id = %s
        """
        result = self.execute_query(query, (admin_user_id,))
        return {"username": result[0][0], "full_name": result[0][1], "created_at": result[0][2], "last_login": result[0][3]} if result else None


    def get_admin_user_devices(self, admin_user_id):
        """Get all devices owned by an admin user"""
        query = """
            SELECT id, email_address, authorization_status, device_type
            FROM devices
            WHERE admin_user_id = %s
            ORDER BY created_at DESC
        """
        return self.execute_query(query, (admin_user_id,))

    
    def update_admin_user_password(self, admin_user_id, new_password):
        """Update admin user password"""
        password_hash = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        query = """
            UPDATE admin_users
            SET password_hash = %s
            WHERE id = %s
        """

        return self.execute_query(query, (password_hash, admin_user_id))


    def get_all_admin_users(self):
        """Get all admin users (for super admin management)"""
        query = """
            SELECT id, username, email, full_name, created_at, last_login, is_active
            FROM admin_users
            ORDER BY created_at DESC
        """
        return self.execute_query(query)


    def add_device(self, admin_user_id, email_address, access_token=None, refresh_token=None):
        """Add a new email address to the database"""
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


    def change_device_status(self, device_id, auth_status):
        """Change device status"""

        assert auth_status in ['inserted', 'authorized', 'non_active']

        query = """
            UPDATE devices
            SET authorization_status = %s
            WHERE id = %s;
        """
        result = self.execute_query(query, (auth_status, device_id))
        if result:
            print(f"Status changed to {auth_status} for email {device_id}.")
        return result


    def get_daily_summaries(self, email_id, start_date=None, end_date=None):
        """
        Gets the daily summaries of a user within a date range.

        Args:
            email_id (int): User ID
            start_date (datetime): Start date (inclusive)
            end_date (datetime): End date (inclusive)

        Returns:
            list: List of tuples with daily data, ordered by date
        """

        query = """
            SELECT * FROM daily_summaries
            WHERE email_id = %s
        """

        params = [email_id]

        if start_date:
            query += " AND date >= %s"
            params.append(start_date.date())
        if end_date:
            query += " AND date <= %s"
            params.append(end_date.date())

        query += " ORDER BY date ASC"

        result = self.execute_query(query, params)
        return result if result else []

    def get_intraday_metrics(self, device_id, metric_type, start_time=None, end_time=None):
        """Gets the intraday metrics associated to an email address."""
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

    
    def get_last_synch(self, device_id):
        """
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


    def get_daily_summary_checkpoint(self, device_id):
        """
        Get the last date for which a daily summary was collected.
        Returns a date object or None.
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


    def get_intraday_checkpoint(self, device_id):
        """
        Get the last date for which intraday data was collected.
        Returns a date object or None.
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


    def get_sleep_checkpoint(self, device_id):
        """
        Get the last date for which a daily summary was collected.
        Returns a date object or None.
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


    def update_last_synch(self, device_id, timestamp):
        """
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

    
    def update_daily_summaries_checkpoint(self, device_id, date):
        """
        Get the last date for which intraday data was collected.
        Returns a date object or None.
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

    
    def update_intraday_checkpoint(self, email_id, timestamp):
        """
        Get the last date for which intraday data was collected.
        Returns a date object or None.
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


    def update_sleep_checkpoint(self, device_id, date):
        """
        Get the last date for which sleep was collected.
        Returns a date object or None.
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


    def get_sleep_logs(self, email_id, start_date=None, end_date=None):
        """Gets the sleep records associated to an email address."""
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

        return self.execute_query(query, params)


    def get_user_alerts(self, email_id, start_time=None, end_time=None, acknowledged=None):
        """Gets the alerts associated to an email address."""
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


    def insert_alert(self, email_id, alert_type, priority, triggering_value, threshold, timestamp=None, details=None):
        """
        Inserts a new alert into the database.

        Args:
        email_id (int): Email ID
            alert_type (str): Type of alert
            priority (str): Alert priority (high, medium, low)
            triggering_value (float): Value that triggered the alert
            threshold (str): Alert threshold (can be a range like "30-200")
            timestamp (datetime, optional): Timestamp of the alert
           *details (str, optional): Additional details about the alert

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


    def get_alert_by_id(self, alert_id):
        """Get a specific alert by its ID"""
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

    def store_pending_auth(self, device_id, state, code_verifier):
        """Store pending authorization with expiration"""
        query = """
            INSERT INTO pending_authorizations (device_id, state, code_verifier, expires_at)
            VALUES (%s, %s, %s, NOW() + INTERVAL '10 minutes')
        """
        return self.execute_query(query, (device_id, state, code_verifier))

    def get_pending_auth(self, state):
        """Retrieve pending authorization if not expired"""
        query = """
            SELECT code_verifier, device_id
            FROM pending_authorizations
            WHERE state = %s AND expires_at > NOW()
        """
        result = self.execute_query(query, (state,))
        if result:
            return {'code_verifier': result[0][0], 'device_id': result[0][1]}
        return None

    def check_pending_auth(self, device_id):
        """Check if there is a pending authorization for the mail of an inserted device"""
        query = """
            SELECT *
            FROM pending_authorizations
            WHERE device_id = %s AND expires_at > NOW()
        """
        result = self.execute_query(query, (device_id,))
        if result:
            return True
        return False

    def delete_pending_auth(self, state):
        """Delete used pending authorization"""
        query = "DELETE FROM pending_authorizations WHERE state = %s"
        return self.execute_query(query, (state,))

    def get_device_by_email_address(self, email_address):
        """Retrieves device id by its name"""
        query = """
            SELECT id FROM devices
            WHERE email_address = %s
            ORDER BY created_at DESC
            LIMIT 1;
        """
        result = self.execute_query(query, (email_address,))
        return result[0][0] if result else None

    def get_device_tokens(self, device_id):
        """Retrieve and decrypt tokens for a device"""
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


    def update_device_tokens(self, device_id, access_token, refresh_token):
        """Updates the access and refresh tokens of a device"""

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


    def update_device_type(self, device_id, device_type):
        """Updates the device_type of a device"""

        query = """
            UPDATE devices
            SET device_type = %s
            WHERE id = %s;
        """
        result = self.execute_query(query, (device_type, device_id))
        return result


    def check_intraday_timestamp(self, device_id, timestamp):
        """Checks if intraday timestamp is already present"""
        query = """
            SELECT * FROM intraday_metrics
            WHERE device_id = %s
            AND time = %s
        """
        result = self.execute_query(query, (device_id, timestamp))
        return bool(result)


    def insert_intraday_metric(self, device_id, timestamp, data_type='heart_rate', value=None):
        """Inserts intraday data into the database"""
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


    def insert_sleep_session(self, device_id):
        query = """
            INSERT INTO sleep_sessions (device_id) 
            VALUES (%s)
            RETURNING id;
        """

        result = self.execute_query(query, (device_id, ))
        if result:
            print(f"Sleep session inserted for device {device_id}")
        return result[0][0]


    def insert_sleep_log(self, sleep_session_id, data):
        """Inserts a sleep record into the database"""

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


    def insert_sleep_level(self, sleep_session_id, data):
        """Inserts a sleep levelrecord into the database"""
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

    
    def insert_sleep_short_level(self, sleep_session_id, short):
        """Inserts a sleep short level record into the database"""
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


    def get_device_history(self, device_id):
        """Retrieves the complete history of a device"""
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

    # def get_all_emails(self):
    #     """Retrieves a list of unique email addresses from the database"""

    #     query = "SELECT id, address_name, status FROM email_addresses;"
    #     result = self.execute_query(query)
    #     return [{
    #                 'id': row[0], 
    #                 'address_name': row[1]
    #             } for row in result if row[2] == 'authorized'] if result else []


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