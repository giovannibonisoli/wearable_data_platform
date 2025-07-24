import psycopg2
from psycopg2 import sql
from config import DB_CONFIG
from encryption import encrypt_token, decrypt_token
import random
from datetime import datetime, timedelta

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
            if self.cursor.description:  # Si la consulta retorna resultados
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

    def get_user_by_email(self, email):
        """Fetch a user by email."""
        query = """
            SELECT id, name, email, access_token, refresh_token
            FROM users
            WHERE email = %s
            ORDER BY created_at DESC
            LIMIT 1
        """
        result = self.execute_query(query, (email,))
        return result[0] if result else None

    def add_user(self, name, email, access_token=None, refresh_token=None):
        """Add a new user to the database"""
        if access_token and refresh_token:
            encrypted_access_token = encrypt_token(access_token)
            encrypted_refresh_token = encrypt_token(refresh_token)
        else:
            encrypted_access_token = None
            encrypted_refresh_token = None

        query = """
            INSERT INTO users (name, email, access_token, refresh_token)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """
        result = self.execute_query(query, (name, email, encrypted_access_token, encrypted_refresh_token))
        return result[0][0] if result else None

    def get_daily_summaries(self, user_id, start_date=None, end_date=None):
        """
        Gets the daily summaries of a user within a date range.

        Args:
            user_id (int): User ID
            start_date (datetime): Start date (inclusive)
            end_date (datetime): End date (inclusive)

        Returns:
            list: List of tuples with daily data, ordered by date
        """

        query = """
            SELECT * FROM daily_summaries
            WHERE user_id = %s
        """

        params = [user_id]

        if start_date:
            query += " AND date >= %s"
            params.append(start_date.date())
        if end_date:
            query += " AND date <= %s"
            params.append(end_date.date())

        query += " ORDER BY date ASC"

        result = self.execute_query(query, params)
        return result if result else []

    def get_intraday_metrics(self, user_id, metric_type, start_time=None, end_time=None):
        """Gets the intraday metrics of a user."""
        query = """
            SELECT time, value FROM intraday_metrics
            WHERE user_id = %s AND type = %s
        """
        params = [user_id, metric_type]

        if start_time:
            query += " AND time >= %s"
            params.append(start_time)

        if end_time:
            query += " AND time <= %s"
            params.append(end_time)

        query += " ORDER BY time"

        return self.execute_query(query, params)

    def get_sleep_logs(self, user_id, start_date=None, end_date=None):
        """Gets the sleep records of a user."""
        query = """
            SELECT * FROM sleep_logs
            WHERE user_id = %s
        """
        params = [user_id]

        if start_date:
            query += " AND start_time >= %s"
            params.append(start_date)

        if end_date:
            query += " AND start_time <= %s"
            params.append(end_date)

        query += " ORDER BY start_time DESC"

        return self.execute_query(query, params)

    def get_user_alerts(self, user_id, start_time=None, end_time=None, acknowledged=None):
        """Gets the alerts of a user."""
        query = """
            SELECT * FROM alerts
            WHERE user_id = %s
        """
        params = [user_id]

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

    def insert_alert(self, user_id, alert_type, priority, triggering_value, threshold, timestamp=None, details=None):
        """
        Inserts a new alert into the database.

        Args:
            user_id (int): User ID
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
                    user_id, alert_type, priority, triggering_value, threshold_value, alert_time, details
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            result = self.execute_query(query, (user_id, alert_type, priority, triggering_value, threshold, timestamp, details))
            return result[0][0] if result else None
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def update_user_tokens(self, email, access_token, refresh_token):
        """Update a user's tokens."""
        encrypted_access_token = encrypt_token(access_token)
        encrypted_refresh_token = encrypt_token(refresh_token)

        query = """
            UPDATE users
            SET access_token = %s, refresh_token = %s
            WHERE email = %s
        """
        return self.execute_query(query, (encrypted_access_token, encrypted_refresh_token, email))

    def get_alert_by_id(self, alert_id):
        """Obtiene una alerta específica por su ID"""
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
                # Convertir el resultado a un diccionario
                columns = [desc[0] for desc in self.cursor.description]
                alert = dict(zip(columns, result[0]))
                return alert
            return None
        except Exception as e:
            print(f"Error al obtener alerta por ID: {str(e)}")
            return None

# Función de conveniencia para mantener compatibilidad con el código existente
def connect_to_db():
    """Función de conveniencia para mantener compatibilidad con el código existente."""
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
        print(f"Error al conectar a la base de datos: {e}")
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

        # Create user table
        db.execute_query("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                access_token TEXT,
                refresh_token TEXT,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Create daily summaries table
        db.execute_query("""
            CREATE TABLE IF NOT EXISTS daily_summaries (
                id SERIAL,
                user_id INTEGER REFERENCES users(id),
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
                UNIQUE(user_id, date)
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
                user_id INTEGER REFERENCES users(id),
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

        # Crear sleep log table
        db.execute_query("""
            CREATE TABLE IF NOT EXISTS sleep_logs (
                id SERIAL,
                user_id INTEGER REFERENCES users(id),
                start_time TIMESTAMPTZ NOT NULL,
                end_time TIMESTAMPTZ NOT NULL,
                duration_ms INTEGER,
                efficiency INTEGER,
                minutes_asleep INTEGER,
                minutes_awake INTEGER,
                minutes_in_rem INTEGER,
                minutes_in_light INTEGER,
                minutes_in_deep INTEGER
            );
        """)

        # Convert it to hypertable
        db.execute_query("""
            SELECT create_hypertable('sleep_logs', 'start_time',
                if_not_exists => TRUE,
                migrate_data => TRUE
            );
        """)

        # Create alert table
        db.execute_query("""
            CREATE TABLE IF NOT EXISTS alerts (
                id SERIAL,
                alert_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                user_id INTEGER REFERENCES users(id),
                alert_type VARCHAR(100) NOT NULL,
                priority VARCHAR(20) NOT NULL,
                triggering_value DOUBLE PRECISION,
                threshold_value VARCHAR(50),
                details TEXT,
                acknowledged BOOLEAN DEFAULT FALSE,
                acknowledged_at TIMESTAMPTZ,
                acknowledged_by INTEGER REFERENCES users(id),
                PRIMARY KEY (id, alert_time)
            );
        """)

        # Convert it to hypertable
        db.execute_query("""
            SELECT create_hypertable('alerts', 'alert_time',
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

def get_latest_user_id_by_email(email):
    """
        Retrieves the most recent user_id associated with an email address.
    """

    conn = connect_to_db()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id FROM users
                    WHERE email = %s
                    ORDER BY created_at DESC
                    LIMIT 1;
                """, (email,))
                result = cur.fetchone()
                return result[0] if result else None
        except Exception as e:
            print(f"Error retrieving the most recent user_id: {e}")
        finally:
            conn.close()
    return None


def insert_intraday_data(user_id, timestamp, heart_rate=0, steps=0, calories=0, distance=0, active_zone_minutes=0):
    """
        Inserts intraday data into the database using the new TimeScaleDB schema.

        Args:
            user_id (int): User ID.
            timestamp (datetime): Timestamp of the data.
            heart_rate (int/float):
            steps (int/float):
            calories (int/float):
            distance (int/float):
            active_zone_minutes (int/float):
    """

    conn = connect_to_db()
    if conn:
        try:
            with conn.cursor() as cursor:
                # Insert directly into the intraday_metrics table
                cursor.execute("""
                    INSERT INTO intraday_metrics (user_id, time, heart_rate, steps, calories, distance, active_zone_minutes)
                    VALUES (%s, %s, %s, %s);
                """, (user_id, timestamp, heart_rate, steps, calories, distance, active_zone_minutes))

                conn.commit()
                print(f"Intraday {data_type} data for user {user_id} successfully saved in intraday_metrics.")
        except Exception as e:
            print(f"Error inserting intraday data: {e}")
            conn.rollback()
        finally:
            conn.close()

def save_to_db(user_id, date, **data):
    """
        Saves Fitbit data in the database using the new TimeScaleDB schema.

        Args:
            user_id (int): User ID.
            date (str): Date of the data (YYYY-MM-DD).
            data (dict): Dictionary with Fitbit data.
    """

    connection = connect_to_db()
    if connection:
        try:
            with connection.cursor() as cursor:
                # Insertar datos en la tabla daily_summaries
                insert_query = """
                INSERT INTO daily_summaries (
                    user_id, date, steps, heart_rate, sleep_minutes,
                    calories, distance, floors, elevation, active_minutes,
                    sedentary_minutes, nutrition_calories, water, weight,
                    bmi, fat, oxygen_saturation, respiratory_rate, temperature
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (date, user_id) DO UPDATE SET
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
                cursor.execute(insert_query, (
                    user_id, date,
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

                connection.commit()
                print(f"Datos de usuario {user_id} guardados exitosamente en daily_summaries.")
        except Exception as e:
            print(f"Error al guardar los datos: {e}")
            connection.rollback()
        finally:
            connection.close()

def get_user_tokens(email):
    """
    Retrieve and decrypt tokens for the user with the most recent timestamp or highest user_id.
    """
    conn = connect_to_db()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT access_token, refresh_token
                    FROM users
                    WHERE email = %s
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1;
                """, (email,))
                result = cur.fetchone()
                if result:
                    encrypted_access_token, encrypted_refresh_token = result
                    # Decrypt the tokens
                    access_token = decrypt_token(encrypted_access_token)
                    refresh_token = decrypt_token(encrypted_refresh_token)
                    return access_token, refresh_token
        except Exception as e:
            print(f"Error retrieving user tokens: {e}")
        finally:
            conn.close()
    return None, None

def get_unique_emails():
    """
        Retrieves a list of unique emails from the database.
    """

    connection = connect_to_db()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT DISTINCT email FROM users;")
                emails = [row[0] for row in cursor.fetchall()]
                return emails
        except Exception as e:
            print(f"Error retrieving unique emails: {e}")
        finally:
            connection.close()
    return []

def get_user_id_by_email(email):
    """
        Retrieves the most recent user ID based on their email address.

        Args:
            email (str): User's email address.

        Returns:
            int: Most recent user ID or None if not found.
    """

    connection = connect_to_db()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT id FROM users
                    WHERE email = %s
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1;
                """, (email,))
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as e:
            print(f"Error retrieving the user ID: {e}")
        finally:
            connection.close()
    return None

def update_users_tokens(email, access_token, refresh_token):
    """
        Updates a user's access and refresh tokens.

        Args:
            email (str): User's email address.
            access_token (str): New access token.
            refresh_token (str): New refresh token.
    """

    # Encrypt the tokens before storing them
    encrypted_access_token = encrypt_token(access_token)
    encrypted_refresh_token = encrypt_token(refresh_token)
    user_id=get_user_id_by_email(email)

    connection = connect_to_db()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE users
                    SET access_token = %s, refresh_token = %s
                    WHERE id = %s;
                """, (encrypted_access_token, encrypted_refresh_token, user_id))
                connection.commit()
                print(f"Tokens updated for the user. {email}.")
        except Exception as e:
            print(f"Failed to update the tokens: {e}")
        finally:
            connection.close()

def get_user_history(user_id):
    """
        Retrieves the complete history of a user using the new TimeScaleDB schema.

        Args:
            user_id (int): User's ID.

        Returns:
            list: List of tuples containing the user's history.
    """

    connection = connect_to_db()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM daily_summaries
                    WHERE user_id = %s
                    ORDER BY date;
                """, (user_id,))
                history = cursor.fetchall()
                return history
        except Exception as e:
            print(f"Failed to retrieve the history: {e}")
        finally:
            connection.close()

def get_email_history(email):
    """
        Retrieves the complete history of an email (which may be associated with multiple users) using the new TimeScaleDB schema.

        Args:
            email (str): User's email address.

        Returns:
            list: List of tuples containing the email's history.
    """

    connection = connect_to_db()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT u.name, d.*
                    FROM users u
                    JOIN daily_summaries d ON u.id = d.user_id
                    WHERE u.email = %s
                    ORDER BY u.created_at, d.date;
                """, (email,))
                history = cursor.fetchall()
                return history
        except Exception as e:
            print(f"Failed to retrieve the history: {e}")
        finally:
            connection.close()
    return []

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

    # Case 1: User with initial normal data
    print("1. Creating user with normal data...")
    user_id_1 = add_user(
        name="Juan Pérez",
        email="juan@example.com",
        access_token="token_juan",
        refresh_token="refresh_juan"
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

def insert_daily_summary(user_id, date, **data):
    """
        Inserts or updates a daily summary in the daily_summaries table.

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
        # Insertar datos en la tabla daily_summaries
        insert_query = """
        INSERT INTO daily_summaries (
            user_id, date, steps, heart_rate, sleep_minutes,
            calories, distance, floors, elevation, active_minutes,
            sedentary_minutes, nutrition_calories, water, weight,
            bmi, fat, oxygen_saturation, respiratory_rate, temperature
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (user_id, date) DO UPDATE SET
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

        db.execute_query(insert_query, (
            user_id, date,
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

        return True
    except Exception as e:
        print(f"Error saving the daily summary: {e}")
        return False
    finally:
        db.close()



def check_intraday_timestamp(user_id, timestamp):
    """
        Checks if intraday timestamp is already present

        Args:
            user_id (int): User ID
            timestamp (datetime): Timestamp of the metric.
            end_date (datetime): End date (inclusive)

        Returns:
            list: List of tuples with daily data, ordered by date
    """

    conn = connect_to_db()
    if conn:

        try:
            with conn.cursor() as cursor:
                query = """
                    SELECT * FROM intraday_metrics
                    WHERE user_id = %s
                    AND time = %s
                """

                cursor.execute(query, [user_id, timestamp])
                result = cursor.fetchone()

                return True if result else False

        except Exception as e:
            print(f"Error checking intraday timestamp: {e}")
            return False
        finally:
            conn.close()

    return False


# def insert_intraday_metric(user_id, timestamp, metric_type, value):
#     """
#         Inserts an intraday metric into the intraday_metrics table.
#
#         Args:
#             user_id (int): User ID.
#             timestamp (datetime): Timestamp of the metric.
#             metric_type (str): Type of metric ('heart_rate', 'steps', etc.).
#             value (float): Value of the metric.
#     """
#
#     connection = connect_to_db()
#     if connection:
#
#         if check_intraday_timestamp():
#
#         else:
#
#         try:
#             with connection.cursor() as cursor:
#                 insert_query = """
#                 INSERT INTO intraday_metrics (user_id, time, type, value)
#                 VALUES (%s, %s, %s, %s);
#                 """
#                 cursor.execute(insert_query, (user_id, timestamp, metric_type, value))
#                 connection.commit()
#                 print(f"Intraday metric {metric_type} for user {user_id} saved successfully.")
#         except Exception as e:
#             print(f"Error saving the intraday metric: {e}")
#             connection.rollback()
#         finally:
#             connection.close()


def insert_intraday_metric(user_id, timestamp, data_type='heart_rate', value=None):
    """
        Inserts intraday data into the database using the new TimeScaleDB schema.

        Args:
            user_id (int): User ID.
            timestamp (datetime): Timestamp of the data.
            heart_rate (int/float):
            steps (int/float):
            calories (int/float):
            distance (int/float):
    """


    conn = connect_to_db()
    if conn:

        if check_intraday_timestamp(user_id, timestamp):

            try:
                with conn.cursor() as cursor:
                    # Insert directly into the intraday_metrics table
                    cursor.execute(f"""
                        UPDATE intraday_metrics
                        SET {data_type}=%s
                        WHERE user_id=%s
                        AND time=%s
                    """, (value, user_id, timestamp))

                    conn.commit()
                    print(f"Intraday {data_type} data for user {user_id} successfully updated in intraday_metrics.")
            except Exception as e:
                print(f"Error updating intraday data: {e}")
                conn.rollback()
            finally:
                conn.close()

        else:

            try:
                with conn.cursor() as cursor:
                    # Insert directly into the intraday_metrics table

                    values = {
                                "heart_rate": None,
                                "steps": None,
                                "calories": None,
                                "distance": None
                    }

                    values[data_type] = value

                    cursor.execute("""
                        INSERT INTO intraday_metrics (user_id, time, heart_rate, steps, calories, distance)
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, (user_id, timestamp, values["heart_rate"], values["steps"], values["calories"], values["distance"]))

                    conn.commit()
                    print(f"Intraday {data_type} data for user {user_id} successfully saved in intraday_metrics.")
            except Exception as e:
                print(f"Error inserting intraday data: {e}")
                conn.rollback()
            finally:
                conn.close()


def insert_sleep_log(user_id, start_time, end_time, **data):
    """
        Inserts a sleep record into the database.

        Args:
            user_id (int): User ID.
            start_time (datetime): Sleep start time.
            end_time (datetime): Sleep end time.
            data (dict): Additional sleep data.
    """

    conn = connect_to_db()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO sleep_logs (
                        user_id, start_time, end_time, duration_ms,
                        efficiency, minutes_asleep, minutes_awake,
                        minutes_in_rem, minutes_in_light, minutes_in_deep
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    user_id, start_time, end_time,
                    data.get('duration_ms'),
                    data.get('efficiency'),
                    data.get('minutes_asleep'),
                    data.get('minutes_awake'),
                    data.get('minutes_in_rem'),
                    data.get('minutes_in_light'),
                    data.get('minutes_in_deep')
                ))
                conn.commit()
                print(f"Sleep record inserted for user {user_id}")
        except Exception as e:
            print(f"Error inserting sleep record: {e}")
            conn.rollback()
        finally:
            conn.close()

def get_user_alerts(user_id, start_time=None, end_time=None, acknowledged=None):
    """
        Retrieves a user's alerts.

        Args:
            user_id (int): User ID.
            start_time (datetime, optional): Start date to filter alerts.
            end_time (datetime, optional): End date to filter alerts.
            acknowledged (bool, optional): Filter by acknowledgment status.

        Returns:
            list: List of user alerts.
    """

    conn = connect_to_db()
    if conn:
        try:
            with conn.cursor() as cursor:
                query = """
                    SELECT * FROM alerts
                    WHERE user_id = %s
                """
                params = [user_id]

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
                cursor.execute(query, params)
                alerts = cursor.fetchall()
                return alerts
        except Exception as e:
            print(f"Error retrieving alerts: {e}")
        finally:
            conn.close()
    return []

def get_daily_summaries(user_id, start_date=None, end_date=None):
    """
        Retrieves a user's daily summaries within a date range.

        Args:
            user_id (int): User ID
            start_date (datetime): Start date (inclusive)
            end_date (datetime): End date (inclusive)

        Returns:
            list: List of tuples with daily data, ordered by date
    """

    db = DatabaseManager()
    if not db.connect():
        print("Error al conectar a la base de datos")
        return []

    try:
        query = """
            SELECT * FROM daily_summaries
            WHERE user_id = %s
        """
        params = [user_id]

        if start_date:
            query += " AND date >= %s"
            params.append(start_date.date())
        if end_date:
            query += " AND date <= %s"
            params.append(end_date.date())

        query += " ORDER BY date ASC"

        result = db.execute_query(query, params)
        return result if result else []
    except Exception as e:
        print(f"Error retrieving daily summaries: {e}")
        return []
    finally:
        db.close()

def get_intraday_metrics(user_id, metric_type, start_time=None, end_time=None):
    """
        Retrieves a user's intraday metrics within a time range.

        Args:
            user_id (int): User ID.
            metric_type (str): Type of metric ('heart_rate', 'steps', etc.).
            start_time (datetime, optional): Start time.
            end_time (datetime, optional): End time.

        Returns:
            list: List of tuples with the intraday metrics.
    """

    connection = connect_to_db()
    if connection:
        try:
            with connection.cursor() as cursor:
                query = """
                SELECT time, value FROM intraday_metrics
                WHERE user_id = %s AND type = %s
                """
                params = [user_id, metric_type]

                if start_time:
                    query += " AND time >= %s"
                    params.append(start_time)

                if end_time:
                    query += " AND time <= %s"
                    params.append(end_time)

                query += " ORDER BY time;"

                cursor.execute(query, params)
                metrics = cursor.fetchall()
                return metrics
        except Exception as e:
            print(f"Error retrieving intraday metrics: {e}")
        finally:
            connection.close()
    return []

def get_sleep_logs(user_id, start_date=None, end_date=None):
    """
        Retrieves a user's sleep records within a date range.

        Args:
            user_id (int): User ID.
            start_date (str, optional): Start date (YYYY-MM-DD).
            end_date (str, optional): End date (YYYY-MM-DD).

        Returns:
            list: List of tuples with the sleep records.
    """

    connection = connect_to_db()
    if connection:
        try:
            with connection.cursor() as cursor:
                query = """
                SELECT * FROM sleep_logs
                WHERE user_id = %s
                """
                params = [user_id]

                if start_date:
                    query += " AND start_time >= %s"
                    params.append(start_date)

                if end_date:
                    query += " AND start_time <= %s"
                    params.append(end_date)

                query += " ORDER BY start_time DESC;"

                cursor.execute(query, params)
                logs = cursor.fetchall()
                return logs
        except Exception as e:
            print(f"Error retrieving sleep records: {e}")
        finally:
            connection.close()
    return []

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
                cursor.execute("DROP TABLE IF EXISTS users CASCADE;")

                connection.commit()
                print("Database tables dropped successfully.")

                # Reinitialize the database
                init_db()
                print("Database reinitialized successfully.")

                # Add the test user using DatabaseManager instance
                db = DatabaseManager()
                if db.connect():
                    db.add_user(
                        name="",
                        email="Wearable2LivelyAgeign@gmail.com",
                        access_token="",
                        refresh_token=""
                    )
                    db.close()

                print("Wearable2LivelyAgeign@gmail.com user added successfully.")

                db = DatabaseManager()
                if db.connect():
                    db.add_user(
                        name="",
                        email="Wearable1LivelyAgeign@gmail.com",
                        access_token="",
                        refresh_token=""
                    )
                    db.close()

                print("Wearable1LivelyAgeign@gmail.com user added successfully.")

        except Exception as e:
            print(f"Error resetting database: {e}")
            connection.rollback()
        finally:
            connection.close()

def create_test_data():
    """Creates test data for development"""
    conn = connect_to_db()
    if not conn:
        return False

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO users (name, email, access_token, refresh_token)
                VALUES ('Test User', 'test@example.com', 'test_token', 'test_refresh')
                RETURNING id
            """)
            user_id = cursor.fetchone()[0]

            # Fecha de la alerta (hoy)
            alert_date = datetime.now().date()

            # Crear datos de actividad para los últimos 7 días
            for i in range(7):
                date = alert_date - timedelta(days=i)
                cursor.execute("""
                    INSERT INTO daily_summaries (
                        user_id, date, steps, heart_rate, sleep_minutes,
                        calories, distance, floors, elevation, active_minutes,
                        sedentary_minutes
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, date) DO UPDATE SET
                        steps = EXCLUDED.steps,
                        heart_rate = EXCLUDED.heart_rate,
                        sleep_minutes = EXCLUDED.sleep_minutes,
                        calories = EXCLUDED.calories,
                        distance = EXCLUDED.distance,
                        floors = EXCLUDED.floors,
                        elevation = EXCLUDED.elevation,
                        active_minutes = EXCLUDED.active_minutes,
                        sedentary_minutes = EXCLUDED.sedentary_minutes
                """, (
                    user_id, date,
                    8000 + random.randint(-500, 500),  # steps
                    70 + random.randint(-5, 5),        # heart_rate
                    420 + random.randint(-30, 30),     # sleep_minutes
                    2000 + random.randint(-200, 200),  # calories
                    5.5 + random.uniform(-0.5, 0.5),   # distance
                    10 + random.randint(-2, 2),        # floors
                    100 + random.randint(-10, 10),     # elevation
                    45 + random.randint(-10, 10),      # active_minutes
                    600 + random.randint(-30, 30)      # sedentary_minutes
                ))

                # Create intraday data ONLY for the alert day (today)
                if date == alert_date:
                    for hour in range(24):
                        # Steps every hour
                        time = datetime.combine(date, datetime.min.time()) + timedelta(hours=hour)
                        steps = random.randint(0, 1000)
                        cursor.execute("""
                            INSERT INTO intraday_metrics (user_id, time, type, value)
                            VALUES (%s, %s, %s, %s)
                        """, (user_id, time, 'steps', steps))
                        # Heart rate every hour
                        hr = random.randint(60, 120)
                        cursor.execute("""
                            INSERT INTO intraday_metrics (user_id, time, type, value)
                            VALUES (%s, %s, %s, %s)
                        """, (user_id, time, 'heart_rate', hr))
                        # Calories every hour
                        calories = random.randint(50, 200)
                        cursor.execute("""
                            INSERT INTO intraday_metrics (user_id, time, type, value)
                            VALUES (%s, %s, %s, %s)
                        """, (user_id, time, 'calories', calories))

            # Create test alerts for today's date
            alert_types = [
                ('activity_drop', 'Low activity level detected'),
                ('sedentary_increase', 'Significant increase in sedentary time'),
                ('sleep_duration_change', 'Significant change in sleep duration'),
                ('heart_rate_anomaly', 'Anomaly detected in heart rate')
            ]
            for i in range(3):
                alert_time = datetime.combine(alert_date, datetime.min.time()) + timedelta(hours=8*i)
                alert_type, message = random.choice(alert_types)
                cursor.execute("""
                    INSERT INTO alerts (
                        user_id, alert_time, alert_type, priority, details
                    ) VALUES (%s, %s, %s, %s, %s)
                """, (
                    user_id, alert_time, alert_type,
                    random.choice(['low', 'medium', 'high']),
                    message
                ))
            # Unacknowledged high-priority alert for today
            cursor.execute("""
                INSERT INTO alerts (
                    user_id, alert_time, alert_type, priority, details, acknowledged
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                user_id, datetime.combine(alert_date, datetime.min.time()) + timedelta(hours=17),
                'heart_rate_anomaly', 'high',
                'Abnormally high heart rate',
                False
            ))
            conn.commit()
            print("Test data created successfully")
            return True
    except Exception as e:
        conn.rollback()
        print(f"Test data created successfully: {str(e)}")
        return False
    finally:
        conn.close()

def drop_intraday_data():

    connection = connect_to_db()
    if connection:
        try:
            with connection.cursor() as cursor:
                print(f"Dropping intraday table")
                query = "DELETE FROM intraday_metrics WHERE user_id=3;"

                cursor.execute(query, [])
                connection.commit()

        except Exception as e:
            print(f"Error while dropping intraday table: {e}")
        finally:
            connection.close()


if __name__ == "__main__":
    # Reset and reinitialize the database
    reset_database()
    # Create test data
    # create_test_data()
    # drop_intraday_data()
