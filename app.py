from logging.handlers import RotatingFileHandler
from flask import Flask, logging, render_template, request, redirect, session, url_for, flash, g, jsonify, Response
from rich import _console
from auth import generate_state, get_tokens, generate_code_verifier, generate_code_challenge, generate_auth_url
from db import DatabaseManager
from config import CLIENT_ID, REDIRECT_URI
from translations import TRANSLATIONS
from emails import send_email


from flask_login import current_user, login_user, logout_user, login_required
from flask_login import LoginManager, UserMixin
from datetime import datetime, timedelta, timezone, time
from flask_babel import Babel, get_locale, gettext as _

import os
import logging
import json
import base64


# Initialize Flask app
app = Flask(__name__,
           static_url_path='/livelyageing/static',  # Prefix for static files with livelyageing
           static_folder='static')  # Directory where static files are stored


app.secret_key = os.getenv('SECRET_KEY')

# Basic logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]',
    handlers=[
        logging.StreamHandler(),  # Log on console
        logging.FileHandler('app.log', mode='w')  # Log on a file
    ]
)

# Get execution mode
FLASK_ENV = os.getenv('FLASK_ENV', 'development')  # By default, development mode
USERNAME = os.getenv('log_USERNAME')
PASSWORD = os.getenv('PASSWORD')


# Language settings
LANGUAGES = {
    'es': 'Español',
    'en': 'English'
}
DEFAULT_LANGUAGE = 'en'

# Initialize Babel
babel = Babel(app)

def get_locale():
    """Get the best language for the user."""
    # First try to get language from the session
    if 'language' in session:
        return session['language']
    # Then try to get it from the user's browser settings
    return request.accept_languages.best_match(LANGUAGES.keys(), DEFAULT_LANGUAGE)

# Configure Babel
app.config['BABEL_DEFAULT_LOCALE'] = DEFAULT_LANGUAGE
app.config['BABEL_TRANSLATION_DIRECTORIES'] = 'translations'
babel.init_app(app, locale_selector=get_locale)

@app.context_processor
def inject_globals():
    """Make common variables available to all templates."""
    return {
        'LANGUAGES': LANGUAGES,
        'get_locale': lambda: str(get_locale()),
        'current_language': lambda: session.get('language', DEFAULT_LANGUAGE)
    }

# Configurar Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'  # Route for starting session


if FLASK_ENV == 'production':
    # Production mode: use public IP and HTTPS.
    HOST = os.getenv('PRODUCTION_HOST','0.0.0.0')
    PORT = int(os.getenv('PRODUCTION_PORT'))
    # SSL_CONTEXT = (
    #     os.getenv('SSL_CERT'),  # Path to the certificate.
    #     os.getenv('SSL_KEY')     # Path to the private key.
    # )
    DEBUG = True
else:
    # Development mode: use localhost and HTTP
    HOST = os.getenv('HOST')
    PORT = int(os.getenv('PORT'))
    SSL_CONTEXT = None
    DEBUG = os.getenv('DEBUG').lower() == 'true'

# User Model
class User(UserMixin):
    def __init__(self, id):
        self.id = id

# Load user
@login_manager.user_loader
def load_user(user_id):
    return User(user_id)

# # Login path
# @app.route('/livelyageing/login', methods=['GET', 'POST'])
# def login():
#     if current_user.is_authenticated:
#         return redirect(url_for('home'))  # Redirect to home instead of index

#     if request.method == 'POST':

#         username = request.form['username']
#         password = request.form['password']

#         if username == USERNAME and password == PASSWORD:
#             user = User(username)
#             login_user(user)
#             return redirect(url_for('home'))  # Redirect to home instead of index

#         else:
#             flash('Incorrect username or password.', 'danger')
#     return render_template('login.html')


@app.route('/livelyageing/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('home'))

    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        db = DatabaseManager()
        if db.connect():
            try:
                user_data = db.verify_admin_user(username, password)
                if user_data:
                    user = User(
                        user_data['id']
                    )
                    login_user(user)
                    
                    # Store user info in session for easy access
                    session['admin_user_id'] = user_data['id']
                    session['username'] = user_data['username']
                    
                    flash(f'Welcome, {user_data["full_name"] or username}!', 'success')
                    return redirect(url_for('home'))
                else:
                    flash('Incorrect username or password.', 'danger')
            finally:
                db.close()
        else:
            flash('Database connection error.', 'danger')
    
    return render_template('login.html')


# Logout path
@app.route('/livelyageing/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

# Protect all routes with `@login_required`.
# @app.before_request
# def require_login():
#     if not current_user.is_authenticated and request.endpoint != 'login':
#         return redirect(url_for('login'))


@app.before_request
def require_login():
    print(f"🔍 Endpoint richiesto: {request.endpoint}")  # ← Debug
    print(f"🔐 Utente autenticato: {current_user.is_authenticated}")  # ← Debug

    public_endpoints = ['login', 'callback', 'static']

    if not current_user.is_authenticated and request.endpoint not in public_endpoints:
        print(f"❌ Bloccato! Redirezione a login")  # ← Debug
        return redirect(url_for('login'))

    print(f"✅ Accesso consentito")

# Route: Root URL redirect
@app.route('/')
def root():
    """
    Redirect from root URL to the home page.
    """
    return redirect(url_for('home'))

# Route: Homepage
@app.route('/livelyageing/')
@login_required
def index():
    """
    Redirect to home page.
    """
    return redirect(url_for('home'))

@app.route('/livelyageing/preload_dashboard')
@login_required
def preload_dashboard():
    """
    Preload dashboard data and store it in session.
    This route should be called via AJAX when the user is likely to access the dashboard.
    """
    db = DatabaseManager()
    if db.connect():
        try:
            # Get the latest daily summary for each user
            daily_summaries = db.execute_query("""
                SELECT u.name, u.email, d.*
                FROM users u
                LEFT JOIN daily_summaries d ON u.id = d.user_id
                WHERE d.date = (SELECT MAX(date) FROM daily_summaries WHERE user_id = u.id)
                OR d.date IS NULL
                ORDER BY d.date DESC NULLS LAST
            """)

            # Get the latest intraday metrics for each user
            intraday_metrics = db.execute_query("""
                SELECT u.name, u.email, i.type, i.value, i.time
                FROM users u
                LEFT JOIN intraday_metrics i ON u.id = i.user_id
                WHERE i.time = (SELECT MAX(time) FROM intraday_metrics WHERE user_id = u.id AND type = i.type)
                OR i.time IS NULL
                ORDER BY i.time DESC NULLS LAST
            """)

            # Get the latest sleep logs for each user
            sleep_logs = db.execute_query("""
                SELECT u.name, u.email, s.*
                FROM users u
                LEFT JOIN sleep_logs s ON u.id = s.user_id
                WHERE s.start_time = (SELECT MAX(start_time) FROM sleep_logs WHERE user_id = u.id)
                OR s.start_time IS NULL
                ORDER BY s.start_time DESC NULLS LAST
            """)

            # Transform intraday_metrics to new 4-column format for dashboard
            intraday_metrics_4col = []
            for metric in intraday_metrics:
                dt = metric[4]
                metric_type = metric[2]
                value = metric[3]
                intraday_metrics_4col.append([
                    dt.date().isoformat(),
                    dt.time().isoformat(timespec='minutes'),
                    metric_type,
                    value
                ])

            # Initialize empty filters_dict and alerts
            filters_dict = {}
            alerts = []

            # Store the processed data in the session for later use
            session['dashboard_data'] = {
                'daily_summaries': daily_summaries,
                'intraday_metrics': intraday_metrics_4col,
                'sleep_logs': sleep_logs,
                'filters_dict': filters_dict,
                'alerts': alerts,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }

            return jsonify({'success': True, 'timestamp': session['dashboard_data']['timestamp']})

        except Exception as e:
            app.logger.error(f"Error fetching data for dashboard: {e}")
            return jsonify({'error': str(e)}), 500
        finally:
            db.close()

    return jsonify({'error': 'Database connection error'}), 500

@app.route('/livelyageing/check_dashboard_updates')
@login_required
def check_dashboard_updates():
    """
    Check if there are any updates to the dashboard data since the last preload.
    """
    last_timestamp = request.args.get('timestamp')
    if not last_timestamp:
        return jsonify({'error': 'No timestamp provided'}), 400

    try:
        last_timestamp = datetime.fromisoformat(last_timestamp)
        current_time = datetime.now(timezone.utc)

        # Check if we need to refresh (more than 5 minutes old)
        if (current_time - last_timestamp).total_seconds() > 300:
            return jsonify({'needs_refresh': True})

        # Check for new alerts
        db = DatabaseManager()
        if db.connect():
            try:
                new_alerts = db.execute_query("""
                    SELECT COUNT(*)
                    FROM alerts
                    WHERE alert_time > %s
                """, (last_timestamp,))

                if new_alerts and new_alerts[0][0] > 0:
                    return jsonify({'needs_refresh': True})

                return jsonify({'needs_refresh': False})
            finally:
                db.close()

        return jsonify({'error': 'Database connection error'}), 500

    except Exception as e:
        app.logger.error(f"Error checking dashboard updates: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/livelyageing/home')
@login_required
def home():
    """
    Render the home page with recent activity.
    """
    db = DatabaseManager()
    return render_template('home.html')



# Route: List of all available devices
# @app.route('/livelyageing/available_email_addresses', methods=['GET', 'POST'])
# @login_required
# def available_email_addresses():

#     db = DatabaseManager()
#     if db.connect():
#         if request.method == 'POST':
#             address_name = request.form['addressName']
#             db.add_email_address(address_name)
#             return redirect(url_for('available_email_addresses'))
#         else:
#             try:
#                 # Get recent users with their latest activity (only users with names AND valid tokens)
#                 result = db.execute_query("""
#                     SELECT id, address_name, status
#                     FROM email_addresses
#                     ORDER BY id DESC;
#                 """)

#                 email_addresses = []
#                 for email_address in result:

#                     status = email_address[2]

#                     if email_address[2] == 'inserted':
#                         if db.check_pending_auth(email_address[0]):
#                             status = 'pending_auth_request'

#                     email_addresses.append({
#                         "id": email_address[0],
#                         "address_name": email_address[1],
#                         "status": status
#                     })

#                 email_addresses.reverse()

#                 return render_template('available_email_addresses.html', email_addresses=email_addresses)
#             except Exception as e:
#                 app.logger.error(f"Error fetching data about available email address: {e}")
#                 return "Error! Error fetching data about available email address.", 500
#             finally:
#                 db.close()
#     else:
#         return "Error! Unable to connect with the database", 500


@app.route('/livelyageing/available_email_addresses', methods=['GET', 'POST'])
@login_required
def available_email_addresses():
    db = DatabaseManager()
    if db.connect():
        try:
            if request.method == 'POST':
                address_name = request.form['addressName']
                # Add email address linked to current user
                email_id = db.add_email_address(
                    current_user.id, 
                    address_name
                )

                print("EMAIL ID:", email_id)
                if email_id:
                    flash(f'Email address {address_name} added successfully!', 'success')
                else:
                    flash('Error adding email address.', 'danger')
                return redirect(url_for('available_email_addresses'))
            else:
                # Get only the email addresses owned by current user
                result = db.get_admin_user_email_addresses(current_user.id)
                
                email_addresses = []
                for email_address in result:
                    status = email_address[2]
                    
                    if status == 'inserted':
                        if db.check_pending_auth(email_address[0]):
                            status = 'pending_auth_request'
                    
                    email_addresses.append({
                        "id": email_address[0],
                        "address_name": email_address[1],
                        "status": status,
                        "created_at": email_address[3]
                    })
                
                return render_template(
                    'available_email_addresses.html', 
                    email_addresses=email_addresses,
                    user=current_user
                )
        except Exception as e:
            app.logger.error(f"Error fetching email addresses: {e}")
            flash('Error loading email addresses.', 'danger')
            return redirect(url_for('home'))
        finally:
            db.close()
    else:
        flash('Database connection error.', 'danger')
        return redirect(url_for('home'))


@app.route('/livelyageing/user_stats')
@login_required
def user_stats():
    """
    Display statistics for all users, organized into three categories:
    1. Active Users: Have name, tokens, and data
    2. Unassigned Users: Latest instance without name/tokens
    3. Historical Users: Previous instances with name and data
    """
    search = request.args.get('search', '').strip()

    db = DatabaseManager()
    if db.connect():
        try:
            # Get all users with relevant information.

            if search:
                users = db.execute_query("""
                    WITH UserInstances AS (
                        SELECT
                            u.id,
                            u.name,
                            u.email,
                            u.created_at,
                            u.access_token IS NOT NULL AND u.refresh_token IS NOT NULL as has_tokens,
                            (SELECT MAX(date) FROM daily_summaries d WHERE d.user_id = u.id) as last_update,
                            EXISTS(SELECT 1 FROM daily_summaries d WHERE d.user_id = u.id) as has_data,
                            ROW_NUMBER() OVER (PARTITION BY u.email ORDER BY u.created_at DESC) as rn
                        FROM users u
                        WHERE LOWER(u.name) LIKE LOWER(%s) OR LOWER(u.email) LIKE LOWER(%s)
                    )
                    SELECT *
                    FROM UserInstances
                    ORDER BY email, created_at DESC
                """, (f"%{search}%", f"%{search}%"))
            else:
                users = db.execute_query("""
                    WITH UserInstances AS (
                        SELECT
                            u.id,
                            u.name,
                            u.email,
                            u.created_at,
                            u.access_token IS NOT NULL AND u.refresh_token IS NOT NULL as has_tokens,
                            (SELECT MAX(date) FROM daily_summaries d WHERE d.user_id = u.id) as last_update,
                            EXISTS(SELECT 1 FROM daily_summaries d WHERE d.user_id = u.id) as has_data,
                            ROW_NUMBER() OVER (PARTITION BY u.email ORDER BY u.created_at DESC) as rn
                        FROM users u
                    )
                    SELECT *
                    FROM UserInstances
                    ORDER BY email, created_at DESC
                """)

            # Process all users
            processed_users = []
            current_email = None

            for user in users:
                user_id, name, email, created_at, has_tokens, last_update, has_data, row_num = user

                # It's the most recent instance if `row_num = 1`.

                is_latest = (row_num == 1)

                # If we change the email or it's the first user.
                if email != current_email:
                    current_email = email

                # Determine the user's status.
                if is_latest:
                    if not name:
                        # If it doesn’t have a name, it’s unassigned.
                        status = 'unassigned'
                    elif not has_tokens:
                        # If it has a name but no tokens, it’s unlinked.
                        status = 'unlinked'
                    elif has_tokens and name:
                        # If it has a name and tokens, it’s active.
                        status = 'active'
                else:
                    # Previous instances are historical if they have a name and data.
                    status = 'historical'

                # Add the user if:
                # 1. It is the most recent instance, OR
                # 2. It is a historical instance that had a name and data

                if is_latest or (name and has_data):
                    processed_users.append({
                        'id': user_id,
                        'name': name,
                        'email': email,
                        'created_at': created_at,
                        'last_update': last_update,
                        'has_tokens': has_tokens,
                        'has_data': has_data,
                        'is_latest': is_latest,
                        'status': status
                    })

            return render_template('user_stats.html',
                                users=processed_users,
                                search=search,
                                now=datetime.now())
        except Exception as e:
            app.logger.error(f"Error fetching user statistics: {e}")
            return "Error: Could not retrieve user statistics.", 500
        finally:
            db.close()
    else:
        return "Error: Could not connect to the database.", 500


@app.route('/livelyageing/send_auth_email', methods=['POST'])
@login_required
def send_auth_email():
    """Generate authorization url and send it by email"""
    email_id = request.form.get('addressIdAuth')
    address_name = request.form.get('addressNameAuth')
    if not address_name:
        flash('Please select an email.', 'danger')
        return redirect(url_for('available_email_addresses'))

    # Generate code_verifier and store it temporarily with email as key
    code_verifier = generate_code_verifier()

    # Create state that includes email (encoded for security)
    state_data = {
        'email': address_name,
        'random': generate_state()  # mantieni randomness per sicurezza
    }
    # Encode the state data
    state = base64.urlsafe_b64encode(json.dumps(state_data).encode()).decode()

    code_challenge = generate_code_challenge(code_verifier)
    auth_url = generate_auth_url(code_challenge, state)

    email_subject = 'Autorizzazione Fitbit - Lively Ageing'

    # Email content
    email_html = f"""
        <html>
          <body>
            <h2>Autorizzazione Fitbit</h2>
            <p>Ciao,</p>
            <p>Per autorizzare l'accesso ai tuoi dati Fitbit, clicca sul link qui sotto:</p>
            <p><a href="{auth_url}">Autorizza Fitbit</a></p>
            <p>Oppure copia e incolla questo link nel tuo browser:</p>
            <p>{auth_url}</p>
            <br>
            <p>Grazie,<br>Team Lively Ageing</p>
          </body>
        </html>
        """

    # Email content in simple text
    email_text = f"""
        Autorizzazione Fitbit

        Ciao,

        Per autorizzare l'accesso ai tuoi dati Fitbit, copia e incolla questo link nel tuo browser:

        {auth_url}

        Grazie,
        Team Lively Ageing
        """

    if send_email(address_name, email_subject, email_html, email_text):
        # Store code_verifier in database or cache with state as key
        db = DatabaseManager()
        if db.connect():
            try:
                # Save the code verifier temporarly (it expires in 10 minutes)
                db.store_pending_auth(email_id, state, code_verifier)
            finally:
                db.close()
        return render_template('auth_email_sent_confirmation.html', address_name=address_name)
    else:
        flash('Error sending email. Please try again.', 'danger')
        return redirect(url_for('available_email_addresses'))


@app.route('/livelyageing/callback')
def callback():
    """
    Handle the callback from Fitbit after the user authorizes the app.
    """
    app.logger.info("Callback route accessed")

    try:
        code = request.args.get('code')
        state = request.args.get('state')

        if not code or not state:
            app.logger.error("Missing code or state parameter")
            flash("Error: Missing authorization information.", "danger")
            return redirect(url_for('available_email_addresses'))

        # Decode state to get email
        try:
            state_data = json.loads(base64.urlsafe_b64decode(state.encode()).decode())
            address_name = state_data.get('email')
        except Exception as e:
            app.logger.error(f"Invalid state parameter: {e}")
            flash("Error: Invalid authorization link.", "danger")
            return redirect(url_for('available_email_addresses'))

        if not address_name:
            app.logger.error("No email found in state")
            flash("Error: Invalid authorization link.", "danger")
            return redirect(url_for('available_email_addresses'))

        db = DatabaseManager()
        if db.connect():
            try:
                # Retrieve code_verifier from database
                pending_auth = db.get_pending_auth(state)
                if not pending_auth:
                    app.logger.error("No pending authorization found or expired")
                    flash("Error: Authorization link expired. Please request a new one.", "danger")
                    return redirect(url_for('available_email_addresses'))

                code_verifier = pending_auth['code_verifier']

                # Get tokens from Fitbit
                access_token, refresh_token = get_tokens(code, code_verifier)
                if not access_token or not refresh_token:
                    raise Exception("Could not retrieve Fitbit tokens.")

                email_id = db.get_email_id_by_name(address_name)
                db.update_email_tokens(email_id, access_token, refresh_token)
                app.logger.info(f"{address_name}'s tokens updated.")

                db.change_email_status(email_id, 'authorized')
                app.logger.info(f"{address_name}'s status updated.")

                db.delete_pending_auth(state)
                app.logger.info(f"Deleted prending request.")

                return render_template('auth_confirmation.html',
                                     address_name=address_name,
                                     success=True,
                                     link_date=datetime.now().strftime('%d/%m/%Y %H:%M'))

            except Exception as e:
                app.logger.error(f"Error during token exchange: {e}")
                return render_template('auth_confirmation.html',
                                     address_name=address_name,
                                     success=False,
                                     error=str(e),
                                     link_date=datetime.now().strftime('%d/%m/%Y %H:%M'))
            finally:
                db.close()
        else:
            app.logger.error("Could not connect to the database.")
            return render_template('auth_confirmation.html',
                                 success=False,
                                 error="Database connection failed",
                                 link_date=datetime.now().strftime('%d/%m/%Y %H:%M'))

    except Exception as e:
        app.logger.error(f"Unexpected error: {e}")
        return render_template('auth_confirmation.html',
                             success=False,
                             error=str(e),
                             link_date=datetime.now().strftime('%d/%m/%Y %H:%M'))

@app.route('/livelyageing/deactivate_email', methods=['POST'])
@login_required
def deactivate_email():
    """ Deactivate authorized mail"""
    email_id = request.form.get('DeactivateId')

    db = DatabaseManager()
    if db.connect():
        db.change_email_status(email_id, 'non_active')

        app.logger.error(f"Email {email_id} deactivated.")

    return redirect(url_for('available_email_addresses'))

# Template filters
@app.template_filter('number')
def format_number(value):
    """Format a number with thousands separator."""
    if value is None:
        return '-'
    try:
        return f"{int(value):,}"
    except (ValueError, TypeError):
        return value

@app.template_filter('datetime')
def format_datetime(value):
    """Format a datetime value."""
    if value is None:
        return '-'
    try:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        elif isinstance(value, int):
            # Convert integer timestamp to datetime
            value = datetime.fromtimestamp(value)
        return value.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        return value

def get_text(key):
    """Get the translation for a key in the current language."""
    lang = str(get_locale())
    # Split the key by dots to access nested dictionaries
    keys = key.split('.')
    value = TRANSLATIONS.get(lang, {}).get(keys[0], {})
    for k in keys[1:]:
        value = value.get(k, '')
    return value if value else key

@app.context_processor
def utility_processor():
    """Make translation function and static URL function available in templates."""
    def static_url(filename):
        """Generate full URL for static files."""
        # Use the complete path including /livelyageing prefix
        return url_for('static', filename=filename)
    return {
        'get_text': get_text,
        'current_language': get_locale,
        'static_url': static_url
    }

@app.route('/livelyageing/change_language')
def change_language():
    """Change the application language."""
    lang = request.args.get('lang', DEFAULT_LANGUAGE)
    if lang in LANGUAGES:
        session['language'] = lang

    # Get the referrer URL
    referrer = request.referrer
    if not referrer:
        return redirect(url_for('home'))

    # Parse the referrer URL to preserve existing query parameters
    from urllib.parse import urlparse, parse_qs, urlencode
    parsed = urlparse(referrer)
    params = parse_qs(parsed.query)

    # Update the lang parameter
    params['lang'] = [lang]

    # Reconstruct the URL with updated parameters
    new_query = urlencode(params, doseq=True)
    path = parsed.path

    return redirect(f"{path}?{new_query}")

@app.route('/livelyageing/refresh_data', methods=['POST'])
@login_required
def refresh_data():
    """
    Refresh Fitbit data for all users.
    """
    try:
        # Get all unique emails from the database
        db = DatabaseManager()
        if not db.connect():
            return jsonify({'error': 'Database connection error'}), 500

        try:
            emails = db.execute_query("SELECT DISTINCT email FROM users")
        finally:
            db.close()

        # Process each email to fetch new data
        from fitbit import process_emails
        # from fitbit_intraday import process_emails as process_intraday_emails

        # Process daily data
        process_emails(emails)
        # Process intraday data
        # process_intraday_emails(emails)

        return jsonify({'success': True})
    except Exception as e:
        app.logger.error(f"Error refreshing data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/livelyageing/api/daily_summary')
@login_required
def get_daily_summary():
    """
    Gets the most recent daily summary of the current user.
    """
    try:
        db = DatabaseManager()
        if not db.connect():
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            user_id = db.get_user_id_by_email(current_user.email)
            if not user_id:
                return jsonify({'error': 'User not found'}), 404

            # Get the most recent summary
            summaries = db.get_daily_summaries(
                email_id=user_id,
                start_date=datetime.now() - timedelta(days=1),
                end_date=datetime.now()
            )
        finally:
            db.close()

        if not summaries:
            return jsonify({'error': 'No data available.'}), 404

        latest_summary = summaries[-1]

        return jsonify({
            'steps': latest_summary[3],
            'heart_rate': latest_summary[4],
            'sleep_minutes': latest_summary[5],
            'calories': latest_summary[6],
            'distance': latest_summary[7],
            'floors': latest_summary[8],
            'elevation': latest_summary[9],
            'active_minutes': latest_summary[10],
            'sedentary_minutes': latest_summary[11],
            'nutrition_calories': latest_summary[12],
            'water': latest_summary[13],
            'weight': latest_summary[14],
            'bmi': latest_summary[15],
            'fat': latest_summary[16],
            'oxygen_saturation': latest_summary[17],
            'respiratory_rate': latest_summary[18],
            'temperature': latest_summary[19]
        })

    except Exception as e:
        app.logger.error(f"Error getting the daily summary.: {str(e)}")
        return jsonify({'error': 'Internal server error.'}), 500

@app.route('/livelyageing/api/alerts')
@login_required
def get_user_alerts_api():
    """
    Gets the most recent alerts of the current user.
    """
    try:
        db = DatabaseManager()
        if not db.connect():
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            user_id = db.get_user_id_by_email(current_user.email)
            if not user_id:
                return jsonify({'error': 'User not found.'}), 404

            # Get alerts from the last 24 hours.
            alerts = db.get_user_alerts(
                email_id=user_id,
                start_time=datetime.now() - timedelta(hours=24),
                end_time=datetime.now(),
                acknowledged=False
            )
        finally:
            db.close()

        return jsonify([{
            'id': alert[0],
            'time': alert[1].isoformat(),
            'type': alert[3],
            'priority': alert[4],
            'triggering_value': alert[5],
            'threshold_value': alert[6],
            'details': alert[7]
        } for alert in alerts])

    except Exception as e:
        app.logger.error(f"Error al obtener las alertas: {str(e)}")
        return jsonify({'error': 'Error interno del servidor'}), 500

@app.route('/livelyageing/dashboard/alerts')
@login_required
def alerts_dashboard():
    try:
        # Check if we have preloaded data in the session
        if 'dashboard_data' in session:
            dashboard_data = session['dashboard_data']
            # Clear the session data after using it
            session.pop('dashboard_data', None)
            return render_template('alerts_dashboard.html',
                                daily_summaries=dashboard_data['daily_summaries'],
                                intraday_metrics=dashboard_data['intraday_metrics'],
                                sleep_logs=dashboard_data['sleep_logs'],
                                filters_dict=dashboard_data['filters_dict'],
                                alerts=dashboard_data['alerts'],
                                now=datetime.now(timezone.utc))

        # If no preloaded data, fetch it from the database
        db = DatabaseManager()
        if not db.connect():
            app.logger.error("Database connection error")
            return jsonify({'error': 'Database connection error'}), 500

        # Get filtering parameters.
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        priority = request.args.get('priority')
        acknowledged = request.args.get('acknowledged')
        user_query = request.args.get('user_query')
        alert_type = request.args.get('alert_type')
        urgent_only = request.args.get('urgent_only') == 'on'
        page = request.args.get('page', 1, type=int)
        per_page = 10  # Number of alerts per page.

        app.logger.info(f"Parámetros de filtrado: date_from={date_from}, date_to={date_to}, priority={priority}, acknowledged={acknowledged}, user_query={user_query}, alert_type={alert_type}, urgent_only={urgent_only}")

        # Create a filter dictionary for pagination.
        filters_dict = {}
        if date_from:
            filters_dict['date_from'] = date_from
        if date_to:
            filters_dict['date_to'] = date_to
        if priority:
            filters_dict['priority'] = priority
        if acknowledged is not None and acknowledged != '':
            filters_dict['acknowledged'] = acknowledged
        if user_query:
            filters_dict['user_query'] = user_query
        if alert_type:
            filters_dict['alert_type'] = alert_type
        if urgent_only:
            filters_dict['urgent_only'] = 'on'

        # Build the base query.
        query = """
            SELECT
                a.id,
                a.alert_time,
                a.user_id,
                a.alert_type,
                a.priority,
                a.triggering_value,
                a.threshold_value,
                a.details,
                a.acknowledged,
                u.name AS user_name,
                u.email AS user_email
            FROM alerts a
            JOIN users u ON a.user_id = u.id
            WHERE 1=1
        """
        params = []

        # Apply filters
        if date_from:
            query += " AND a.alert_time >= %s"
            params.append(f"{date_from} 00:00:00")
        if date_to:
            query += " AND a.alert_time <= %s"
            params.append(f"{date_to} 23:59:59")
        if priority:
            query += " AND a.priority = %s"
            params.append(priority)
        if acknowledged is not None and acknowledged != '':
            query += " AND a.acknowledged = %s"
            params.append(acknowledged == 'true')
        if user_query:
            query += " AND (LOWER(u.name) LIKE LOWER(%s) OR LOWER(u.email) LIKE LOWER(%s))"
            search_term = f"%{user_query}%"
            params.extend([search_term, search_term])
        if alert_type:
            query += " AND a.alert_type LIKE %s"
            params.append(f"%{alert_type}%")
        if urgent_only:
            query += " AND a.acknowledged = FALSE AND a.alert_time <= NOW() - INTERVAL '24 hours'"

        # Sort by priority and descending date.
        query += " ORDER BY CASE a.priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 END, a.alert_time DESC"

        app.logger.info(f"Query: {query}")
        app.logger.info(f"Params: {params}")

        try:
            # Get the total number of alerts for pagination
            count_query = f"SELECT COUNT(*) FROM ({query}) AS count_query"
            total = db.execute_query(count_query, params)[0][0]
            app.logger.info(f"Total de alertas encontradas: {total}")

            # Apply pagination
            query += " LIMIT %s OFFSET %s"
            params.extend([per_page, (page - 1) * per_page])

            # Execute the query
            alerts_data = db.execute_query(query, params)
            app.logger.info(f"Alertas obtenidas: {len(alerts_data) if alerts_data else 0}")

            if not alerts_data:
                app.logger.warning("No se encontraron alertas con los filtros actuales")
                return render_template('alerts_dashboard.html',
                                    alerts=[],
                                    pagination=None,
                                    filters_dict=filters_dict,
                                    now=datetime.now(timezone.utc))

            # Cnvert tuples to dictionaries with attribute names
            alerts = []
            for alert in alerts_data:
                try:
                    # Get intraday data for the alert.
                    intraday_data = {}
                    alert_type = alert[3]
                    base_alert_type = alert_type.split('_')[0] if '_' in alert_type else alert_type

                    # Map base_alert_type to the actual intraday metric
                    intraday_metric_type = None
                    if base_alert_type == 'heart':
                        intraday_metric_type = 'heart_rate'

                    elif base_alert_type == 'activity':
                        # Only show steps if the reason is steps
                        if alert[7] and 'pasos' in alert[7].lower():
                            intraday_metric_type = 'steps'
                            app.logger.info(f"Alert {alert[0]}: activity_drop caused by steps, intraday steps data will be fetched.")
                        else:
                            app.logger.info(f"Alerta {alert[0]}: `activity_drop` NOT caused by steps, intraday data will not be shown.")
                    elif base_alert_type in ['steps', 'calories', 'active_zone_minutes']:

                        intraday_metric_type = base_alert_type

                    elif base_alert_type == 'intraday':
                        # or intraday_activity_drop alerts, always display step data.
                        intraday_metric_type = 'steps'
                        app.logger.info(f"Alert {alert[0]}: {alertType}, intraday step data will be fetched.")

                    if intraday_metric_type:
                        start_time = alert[1] - timedelta(hours=24)
                        end_time = alert[1]
                        app.logger.info(f"Alerta {alert[0]}: fetching {intraday_metric_type} intraday data for user_id={alert[2]} from {start_time} to {end_time}")
                        intraday_metrics = db.execute_query("""
                            SELECT time, value
                            FROM intraday_metrics
                            WHERE user_id = %s
                            AND type = %s
                            AND time BETWEEN %s AND %s
                            ORDER BY time
                        """, (alert[2], intraday_metric_type, start_time, end_time))
                        app.logger.info(f"Alerta {alert[0]}: fetched {len(intraday_metrics) if intraday_metrics else 0} intraday data for {intraday_metric_type}")
                        if intraday_metrics:
                            intraday_data = {
                                'times': [m[0].strftime('%H:%M') for m in intraday_metrics],
                                'values': [float(m[1]) for m in intraday_metrics]
                            }
                            app.logger.info(f"Intraday data retrieved for {intraday_metric_type}: {len(intraday_metrics)} records")
                        else:
                            app.logger.info(f"Intraday data for {intraday_metric_type} not found")

                    # To convert a datetime object to a formatted string in Python, you can use the strftime() method.
                    alert_time = alert[1].strftime('%Y-%m-%d %H:%M')

                    alerts.append({
                        'id': alert[0],
                        'alert_time': alert_time,
                        'user_id': alert[2],
                        'alert_type': alert[3],
                        'priority': alert[4].lower(),
                        'triggering_value': alert[5],
                        'threshold_value': alert[6],
                        'details': alert[7],
                        'acknowledged': alert[8],
                        'user_name': alert[9],
                        'user_email': alert[10],
                        'intraday_data': intraday_data,
                        'raw_alert_time': alert[1]
                    })
                except Exception as e:
                    app.logger.error(f"Error procesando alerta {alert[0]}: {e}")
                    continue

            app.logger.info(f"Processed alerts: {len(alerts)}")

            # Create a pagination object
            pagination = {
                'page': page,
                'per_page': per_page,
                'total': total,
                'pages': (total + per_page - 1) // per_page,
                'has_prev': page > 1,
                'has_next': page * per_page < total,
                'prev_num': page - 1,
                'next_num': page + 1,
                'iter_pages': lambda: range(1, ((total + per_page - 1) // per_page) + 1)
            }

            # Make sure that now is timezone-aware
            now = datetime.now(timezone.utc)

            return render_template('alerts_dashboard.html',
                                alerts=alerts,
                                pagination=pagination,
                                filters_dict=filters_dict,
                                now=now)

        except Exception as e:
            app.logger.error(f"Error in the SQL query: {e}")
            return render_template('alerts_dashboard.html',
                                alerts=[],
                                pagination=None,
                                filters_dict=filters_dict,
                                now=datetime.now(timezone.utc))

    except Exception as e:
        app.logger.error(f"Error al cargar el dashboard de alertas: {e}")
        return render_template('alerts_dashboard.html',
                            alerts=[],
                            pagination=None,
                            filters_dict={},
                            now=datetime.now(timezone.utc))

@app.route('/livelyageing/api/alerts/<int:alert_id>')
@login_required
def get_alert_details(alert_id):
    try:
        db = DatabaseManager()
        if not db.connect():
            return jsonify({'error': 'Database connection error'}), 500

        # Obtain alert details
        query = """
            SELECT
                a.id,
                a.alert_time,
                a.user_id,
                a.alert_type,
                a.priority,
                a.triggering_value,
                a.threshold_value,
                a.details,
                a.acknowledged,
                u.name AS user_name,
                u.email AS user_email
            FROM alerts a
            JOIN users u ON a.user_id = u.id
            WHERE a.id = %s
        """
        result = db.execute_query(query, [alert_id])

        if not result:
            return jsonify({'error': 'Alert not found'}), 404

        alert = {
            'id': result[0][0],
            'alert_time': result[0][1].isoformat(),
            'user_id': result[0][2],
            'alert_type': result[0][3],
            'priority': result[0][4],
            'triggering_value': result[0][5],
            'threshold_value': result[0][6],
            'details': result[0][7],
            'acknowledged': result[0][8],
            'user_name': result[0][9],
            'user_email': result[0][10]
        }

        return jsonify(alert)

    except Exception as e:
        app.logger.error(f"Error obtaining details on the alert: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/livelyageing/api/alerts/<int:alert_id>/acknowledge', methods=['POST'])
@login_required
def acknowledge_alert(alert_id):
    try:
        db = DatabaseManager()
        if not db.connect():
            return jsonify({'success': False, 'error': 'Database connection error'}), 500

        try:
            # Check if the alert exists and is unacknowledged.
            check_query = "SELECT acknowledged FROM alerts WHERE id = %s"
            result = db.execute_query(check_query, [alert_id])

            if not result:
                return jsonify({'success': False, 'error': 'Alert not found'}), 404

            if result[0][0]:
                return jsonify({'success': False, 'error': 'The alert has already been acknowledged.'}), 400

            # Only update 'acknowledged' field
            db.execute_query("""
                UPDATE alerts
                SET acknowledged = TRUE
                WHERE id = %s
            """, [alert_id])

            return jsonify({'success': True})

        finally:
            db.close()

    except Exception as e:
        app.logger.error(f"Error al reconocer alerta: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/livelyageing/user/<int:user_id>')
@login_required
def user_detail(user_id):
    """
    Render the user card with basic information and recent data.
    The remaining data is loaded via AJAX.
    """
    db = DatabaseManager()
    if not db.connect():
        return "Error: Get the user's basic data.", 500
    try:
        # Get the user's basic data
        user_data = db.execute_query(
            """
            SELECT id, name, email, created_at,
                   access_token, refresh_token,
                   EXTRACT(YEAR FROM AGE(CURRENT_DATE, created_at)) as age
            FROM users
            WHERE id = %s
            """, (user_id,)
        )
        if not user_data:
            return "User not found", 404

        # Convert the tuple to a dictionary
        user = {
            'id': user_data[0][0],
            'name': user_data[0][1],
            'email': user_data[0][2],
            'created_at': user_data[0][3],
            'access_token': user_data[0][4],
            'refresh_token': user_data[0][5],
            'age': int(user_data[0][6]) if user_data[0][6] else None
        }

        # Get last daily summary for current data
        latest_summary = db.execute_query(
            """
            SELECT * FROM daily_summaries
            WHERE user_id = %s
            ORDER BY date DESC
            LIMIT 1
            """, (user_id,)
        )
        last_update_datetime = None
        if latest_summary:
            columns = [desc[0] for desc in db.cursor.description]
            latest_summary = dict(zip(columns, latest_summary[0]))
            last_update_datetime = datetime.combine(latest_summary['date'], time(23, 59))
        else:
            last_update_datetime = None

        # Get recent alerts not acknowledged
        recent_alerts = db.execute_query(
            """
            SELECT * FROM alerts
            WHERE user_id = %s
            AND alert_time >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY alert_time DESC
            """, (user_id,)
        )

        # Convert the alerts into dictionaries.
        if recent_alerts:
            alert_columns = [desc[0] for desc in db.cursor.description]
            recent_alerts = [dict(zip(alert_columns, alert)) for alert in recent_alerts]

            # Process active alerts for visual indicators.
            alerts = {
                'activity_drop': False,
                'heart_rate_anomaly': False,
                'sleep_duration_change': False,
                'sedentary_increase': False
            }

            for alert in recent_alerts:
                if not alert['acknowledged'] and alert['alert_time'].date() == datetime.now().date():
                    alert_type = alert['alert_type']
                    if alert_type in alerts:
                        alerts[alert_type] = True
        else:
            alerts = {
                'activity_drop': False,
                'heart_rate_anomaly': False,
                'sleep_duration_change': False,
                'sedentary_increase': False
            }

        return render_template('user_detail.html',
                             user=user,
                             latest_summary=latest_summary,
                             recent_alerts=recent_alerts,
                             alerts=alerts,
                             now=datetime.now(),
                             last_update_datetime=last_update_datetime)
    except Exception as e:
        app.logger.error(f"Error loading the user profile: {e}")
        return "Internal server error", 500
    finally:
        db.close()

@app.route('/livelyageing/api/user/<int:user_id>/daily_summary')
@login_required
def api_user_daily_summary(user_id):
    """
    Returns the daily summary for a user and a date (today by default)
    """
    date_str = request.args.get('date')
    if date_str:
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            return jsonify({'error': 'Invalid date format'}), 400
    else:
        date = datetime.now().date()
    db = DatabaseManager()
    if not db.connect():
        return jsonify({'error': 'DB error'}), 500
    try:
        summary = db.execute_query(
            """
            SELECT
                date,
                steps,
                heart_rate,
                sleep_minutes,
                calories,
                distance,
                floors,
                elevation,
                active_minutes,
                sedentary_minutes,
                nutrition_calories,
                water,
                weight,
                bmi,
                fat,
                oxygen_saturation,
                respiratory_rate,
                temperature
            FROM daily_summaries
            WHERE user_id = %s AND date = %s
            """, (user_id, date)
        )
        if not summary:
            return jsonify({'error': 'No hay datos para ese día'}), 404

        # Mapear los campos a nombres legibles
        columns = [desc[0] for desc in db.cursor.description]
        summary_dict = dict(zip(columns, summary[0]))

        # Calcular valores adicionales
        if summary_dict.get('sleep_minutes'):
            summary_dict['sleep_hours'] = round(summary_dict['sleep_minutes'] / 60, 1)
        if summary_dict.get('sedentary_minutes'):
            summary_dict['sedentary_hours'] = round(summary_dict['sedentary_minutes'] / 60, 1)
        return jsonify({'summary': summary_dict})
    finally:
        db.close()

@app.route('/livelyageing/api/user/<int:user_id>/intraday')
@login_required
def api_user_intraday(user_id):
    """
    Returns intraday data for the users together with date and metric type.
    """
    date_str = request.args.get('date')
    metric_type = request.args.get('type')
    if not metric_type:
        return jsonify({'error': 'The metric type is missing.'}), 400
    if date_str:
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            return jsonify({'error': 'Invalid date format'}), 400
    else:
        date = datetime.now().date()

    db = DatabaseManager()
    if not db.connect():
        return jsonify({'error': 'DB error'}), 500
    try:
        start_time = datetime.combine(date, datetime.min.time())
        end_time = datetime.combine(date, datetime.max.time())
        data = db.execute_query(
            f"""
            SELECT time, {metric_type}
            FROM intraday_metrics
            WHERE user_id = %s
            AND time BETWEEN %s AND %s
            ORDER BY time
            """, (user_id, start_time, end_time)
        )

        # for row in data:
        #     print(row)
        #     print(len(row))


        return jsonify({
            'intraday': [
                {
                    'time': row[0].strftime('%H:%M'),
                    'value': float(row[1] if row[1] is not None else 0)
                } for row in data
            ]
        })
    finally:
        db.close()

@app.route('/livelyageing/api/user/<int:user_id>/weekly_summary')
@login_required
def api_user_weekly_summary(user_id):
    """
    Returns the daily summaries from the last 7 days for the user.
    """
    db = DatabaseManager()
    if not db.connect():
        return jsonify({'error': 'DB error'}), 500
    try:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=6)
        data = db.execute_query(
            """
            SELECT
                date,
                steps,
                heart_rate,
                sleep_minutes,
                calories,
                sedentary_minutes,
                active_minutes,
                distance,
                floors,
                elevation,
                nutrition_calories,
                water,
                weight,
                bmi,
                fat,
                oxygen_saturation,
                respiratory_rate,
                temperature
            FROM daily_summaries
            WHERE user_id = %s
            AND date BETWEEN %s AND %s
            ORDER BY date DESC
            """, (user_id, start_date, end_date)
        )
        return jsonify({
            'weekly': [
                {
                    'date': row[0].strftime('%d/%m'),
                    'steps': row[1],
                    'heart_rate': row[2],
                    'sleep_hours': round(row[3] / 60, 1) if row[3] else None,
                    'calories': row[4],
                    'sedentary_hours': round(row[5] / 60, 1) if row[5] else None,
                    'active_minutes': row[6],
                    'distance': row[7],
                    'floors': row[8],
                    'elevation': row[9],
                    'nutrition_calories': row[10],
                    'water': row[11],
                    'weight': row[12],
                    'bmi': row[13],
                    'fat': row[14],
                    'oxygen_saturation': row[15],
                    'respiratory_rate': row[16],
                    'temperature': row[17]
                } for row in data
            ]
        })
    finally:
        db.close()

@app.route('/livelyageing/api/user/<int:user_id>/alerts')
@login_required
def api_user_alerts(user_id):
    """
    Return the alerts from the last 7 days for the user
    """
    db = DatabaseManager()
    if not db.connect():
        return jsonify({'error': 'DB error'}), 500
    try:
        since = datetime.now() - timedelta(days=7)
        data = db.execute_query(
            """
            SELECT
                alert_time,
                alert_type,
                priority,
                triggering_value,
                threshold_value,
                details,
                acknowledged
            FROM alerts
            WHERE user_id = %s
            AND alert_time >= %s
            ORDER BY alert_time DESC
            """, (user_id, since)
        )

        return jsonify({
            'alerts': [
                {
                    'alert_time': row[0].strftime('%d/%m %H:%M'),
                    'type': row[1],
                    'priority': row[2],
                    'triggering_value': row[3],
                    'threshold_value': row[4],
                    'details': row[5],
                    'acknowledged': row[6]
                } for row in data
            ]
        })
    finally:
        db.close()

@app.route('/livelyageing/dashboard/alerts/export')
@login_required
def export_alerts():
    import csv
    from io import StringIO
    db = DatabaseManager()
    if not db.connect():
        return "Database connection error", 500
    try:
        # Get filters the same way as in alerts_dashboard
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        priority = request.args.get('priority')
        acknowledged = request.args.get('acknowledged')
        user_query = request.args.get('user_query')

        # Build the base query.
        query = """
            SELECT
                a.alert_time,
                u.name AS user_name,
                u.email AS user_email,
                a.alert_type,
                a.priority,
                a.triggering_value,
                a.threshold_value,
                a.details,
                a.acknowledged
            FROM alerts a
            JOIN users u ON a.user_id = u.id
            WHERE 1=1
        """
        params = []
        if date_from:
            query += " AND a.alert_time >= %s"
            params.append(f"{date_from} 00:00:00")
        if date_to:
            query += " AND a.alert_time <= %s"
            params.append(f"{date_to} 23:59:59")
        if priority:
            query += " AND a.priority = %s"
            params.append(priority)
        if acknowledged is not None and acknowledged != '':
            query += " AND a.acknowledged = %s"
            params.append(acknowledged == 'true')
        if user_query:
            query += " AND (LOWER(u.name) LIKE LOWER(%s) OR LOWER(u.email) LIKE LOWER(%s))"
            search_term = f"%{user_query}%"
            params.extend([search_term, search_term])
        query += " ORDER BY a.alert_time DESC"
        alerts = db.execute_query(query, params)

        # Create a CSV with UTF-8 BOM for Excel compatibility.
        si = StringIO()
        cw = csv.writer(si)
        cw.writerow(["Date", "User", "Email", "alertType", "Priority", "Trigger Value", "Threshold", "Details", "Acknowledged"])
        for a in alerts:
            cw.writerow([
                a[0].strftime('%Y-%m-%d %H:%M'),
                a[1], a[2], a[3], a[4], a[5], a[6], a[7], "Sí" if a[8] else "No"
            ])
        output = '\ufeff' + si.getvalue()  # Add BOM UTF-8
        si.close()
        date = datetime.now().strftime('%Y%m%d')
        return Response(
            output,
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment;filename=alertas_{date}.csv"}
        )
    finally:
        db.close()

@app.route('/livelyageing/user/<int:user_id>/export_alerts')
@login_required
def export_user_alerts(user_id):
    import csv
    from io import StringIO
    db = DatabaseManager()
    if not db.connect():
        return "Database connection error", 500
    try:
        since = datetime.now() - timedelta(days=7)
        query = """
            SELECT
                a.alert_time,
                u.name AS user_name,
                u.email AS user_email,
                a.alert_type,
                a.priority,
                a.triggering_value,
                a.threshold_value,
                a.details,
                a.acknowledged
            FROM alerts a
            JOIN users u ON a.user_id = u.id
            WHERE a.user_id = %s AND a.alert_time >= %s
            ORDER BY a.alert_time DESC
        """
        alerts = db.execute_query(query, (user_id, since))
        si = StringIO()
        cw = csv.writer(si)
        cw.writerow(["Date/Hour", "User", "Email", "Alert type", "Priority", "Trigger value", "Threshold", "Details", "Acknowledged"])
        for a in alerts:
            cw.writerow([
                a[0].strftime('%Y-%m-%d %H:%M'),
                a[1], a[2], a[3], a[4], a[5], a[6], a[7], "Sí" if a[8] else "No"
            ])
        output = '\ufeff' + si.getvalue()
        si.close()
        date = datetime.now().strftime('%Y%m%d')
        return Response(
            output,
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment;filename=alerts_user_{user_id}_{date}.csv"}
        )
    finally:
        db.close()

@app.route('/livelyageing/user/<int:user_id>/export_intraday')
@login_required
def export_user_intraday(user_id):
    import csv
    from io import StringIO
    db = DatabaseManager()
    if not db.connect():
        return "Database connection error", 500
    try:
        # Obtain selected dates and metrics
        dates = request.args.getlist('dates')
        metrics = request.args.getlist('metrics')
        if not dates or not metrics:
            return "You must select at least one date and one metric.", 400

        # Set query
        rows = []
        for date_str in dates:
            for metric in metrics:
                start_time = datetime.strptime(date_str, "%Y-%m-%d")
                end_time = start_time + timedelta(days=1)
                query = """
                    SELECT time, type, value
                    FROM intraday_metrics
                    WHERE user_id = %s AND type = %s AND time >= %s AND time < %s
                    ORDER BY time
                """
                data = db.execute_query(query, (user_id, metric, start_time, end_time))
                for row in data:
                    rows.append((row[0].date().strftime('%Y-%m-%d'), row[0].strftime('%H:%M'), row[1], row[2]))
        # Crear CSV
        si = StringIO()
        cw = csv.writer(si)
        cw.writerow(["Date", "Hour", "Metric", "Value"])
        for r in rows:
            cw.writerow(r)
        output = '\ufeff' + si.getvalue()
        si.close()
        date = datetime.now().strftime('%Y%m%d')
        return Response(
            output,
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment;filename=intraday_user_{user_id}_{date}.csv"}
        )
    finally:
        db.close()

@app.route('/livelyageing/unlink_user', methods=['POST'])
@login_required
def unlink_user():
    """
    Handles unlinking a user from their Fitbit device.
    When unlinking:
    1. The original instance is preserved with its name and data (becomes historical)
    2. A new instance is created with the same email but no name/tokens (becomes unassigned)
    """
    user_id = request.form.get('user_id')
    if not user_id:
        flash('User ID not provided', 'danger')
        return redirect(url_for('user_stats'))

    db = DatabaseManager()
    if db.connect():
        try:
            # First, get the email of the user being unlinked
            user_email = db.execute_query("""
                SELECT email FROM users WHERE id = %s
            """, (user_id,))

            if not user_email:
                flash('User not found', 'danger')
                return redirect(url_for('user_stats'))

            email = user_email[0][0]

            # Start a transaction
            db.execute_query("BEGIN")

            try:
                # 1. Create a new unassigned instance with the same email
                db.execute_query("""
                    INSERT INTO users (name, email, access_token, refresh_token)
                    VALUES ('', %s, NULL, NULL)
                """, (email,))

                # 2. Remove tokens from the original instance (but keep name and data)
                db.execute_query("""
                    UPDATE users
                    SET access_token = NULL,
                        refresh_token = NULL
                    WHERE id = %s
                """, (user_id,))

                # Commit the transaction
                db.execute_query("COMMIT")

                flash('Device successfully unlinked. User and historical data are preserved.', 'success')
            except Exception as e:
                # If anything fails, rollback the transaction
                db.execute_query("ROLLBACK")
                app.logger.error(f"Error in unlink transaction: {e}")
                flash('Failed to unlink user', 'danger')
                raise

        except Exception as e:
            app.logger.error(f"Failed to unlink user: {e}")
            flash('Failed to unlink user', 'danger')
        finally:
            db.close()
    else:
        flash('Database connection error', 'danger')

    return redirect(url_for('user_stats'))

@app.route('/livelyageing/debug_static')
def debug_static():
    """Temporary route to debug static file URLs"""
    style_url = url_for('static', filename='css/style.css', _external=True)
    styles_url = url_for('static', filename='css/styles.css', _external=True)
    app.logger.info(f"style.css URL: {style_url}")
    app.logger.info(f"styles.css URL: {styles_url}")
    return {
        'style_url': style_url,
        'styles_url': styles_url
    }

# Run the Flask app
if __name__ == '__main__':
    # app.run(host=HOST, port=PORT, debug=DEBUG)
    app.run(debug=True)
