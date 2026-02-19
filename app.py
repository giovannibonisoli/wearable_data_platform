from logging.handlers import RotatingFileHandler
from flask import Flask, logging, render_template, request, redirect, session, url_for, flash, g, jsonify, Response
from rich import _console

from flask_login import current_user, login_user, logout_user, login_required
from flask_login import LoginManager, UserMixin
from datetime import datetime, timedelta, timezone, time
from flask_babel import Babel, get_locale, format_date, format_datetime, gettext as babel_gettext

from device_statistics import get_device_sync_data, get_last_device_usage_statistics
from auth import generate_state, get_tokens, generate_code_verifier, generate_code_challenge, generate_auth_url, get_device_info
from database import Database, ConnectionManager, AdminUserRepository, DeviceRepository, AuthorizationRepository
from services import DeviceService, DeviceStatisticsService
from config import CLIENT_ID, REDIRECT_URI
from translations import TRANSLATIONS
from emails import send_email

import os
import logging
import json
import base64
import requests


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


# Language settings
LANGUAGES = {
    'it': 'Italiano',
    'en': 'English'
}

DEFAULT_LANGUAGE = 'it'

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


@app.route('/livelyageing/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('home'))

    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        with ConnectionManager() as conn:
            admin_repo = AdminUserRepository(conn)
            user_data = admin_repo.verify_credentials(username, password)

            if user_data:
                user = User(user_data['id'])
                login_user(user)
                    
                # Store user info in session for easy access
                session['admin_user_id'] = user_data['id']
                session['username'] = user_data['username']
                    
                name = user_data["full_name"] or username
                flash_translated('flash.welcome_user', 'success', name=name)
                return redirect(url_for('home'))
            else:
                flash_translated('flash.incorrect_credentials', 'danger')
    
    return render_template('login.html')



# Logout path
@app.route('/livelyageing/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


@app.before_request
def require_login():
    print(f"🔍 Endpoint richiesto: {request.endpoint}")  # ← Debug
    print(f"🔐 Utente autenticato: {current_user.is_authenticated}")  # ← Debug

    public_endpoints = ['login', 'callback', 'static']

    if not current_user.is_authenticated and request.endpoint not in public_endpoints:
        print(f"❌ Bloked! Recirect to login")  # ← Debug
        return redirect(url_for('login'))

    print(f"✅ Access allowed")

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


@app.route('/livelyageing/admin_user_profile')
@login_required
def admin_user_profile():
    try:
        with ConnectionManager() as conn:
            admin_repo = AdminUserRepository(conn)
            device_repo = DeviceRepository(conn)
            
            admin_user_id = int(current_user.id)
            admin_user = admin_repo.get_by_id(admin_user_id)
            devices = device_repo.get_by_admin_user(admin_user_id)
            
            admin_user = {
                'id': admin_user_id,
                'username': admin_user.username,
                'full_name': admin_user.full_name,
                'created_at': admin_user.created_at,
                'last_login': admin_user.last_login,
                'num_devices': len(devices)
            }
            
            return render_template('admin_user_profile.html', admin_user=admin_user)
    except Exception as e:
        app.logger.error(f"Error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/livelyageing/change_password', methods=['POST'])
@login_required
def change_password():
    
    current_password = request.form['current_password']
    new_password = request.form['new_password']
    confirm_password = request.form['confirm_password']

    if len(new_password) >= 8:
        admin_user_id = int(current_user.id)
   
        with ConnectionManager() as conn:
            admin_repo = AdminUserRepository(conn)
            user = admin_repo.get_by_id(admin_user_id)

            if user:
                if new_password == confirm_password:

                    result = admin_repo.update_password(admin_user_id, new_password)
                    
                    if result:
                        flash_translated('flash.password_changed_successfully', 'success')
                    else:
                        flash_translated('flash.password_change_failed', 'danger')
                else:
                    flash_translated('flash.passwords_do_not_match', 'danger')
            else:
                flash_translated('flash.current_password_incorrect', 'danger')
    else:
        flash_translated('flash.password_too_short', 'danger')
    
    return redirect(url_for('admin_user_profile'))


@app.route('/livelyageing/home')
@login_required
def home():
    """
    Display all devices for the logged-in admin.
    
    """

    with ConnectionManager() as conn:
        device_service = DeviceService(conn)
        device_stats_service = DeviceStatisticsService(conn)

        
        try:
            admin_user_id = int(current_user.id)
            devices_data = device_service.get_devices_info_by_admin_user(admin_user_id)
            
            final_devices_data = []
            for device_data in devices_data:
                auth_status = device_data["auth_status"]

                data_reception_status = 'no_data'
                data_reception_details = {}
                device_usage_details = {}

                if device_data["auth_status"] == 'inserted' and device_data["is_pending_auth"]:
                    device_data["auth_status"] = "pending_auth_request"
                    
                elif device_data["auth_status"] == 'authorized':
                    data_reception_status, data_reception_details = device_stats_service.get_device_sync_data(device_data["id"])
                    device_usage_details = device_stats_service.get_last_device_usage_statistics(device_data["id"], timedelta(days=7))
                
                final_devices_data.append({
                        "id": device_data["id"],
                        "email_address": device_data["email_address"],
                        "auth_status": device_data["auth_status"],
                        "device_type": device_data["device_type"],
                        "data_reception_status": data_reception_status,
                        "data_reception_details": data_reception_details,
                        "device_usage_details": device_usage_details
                    })
                
            return render_template('home.html', devices=final_devices_data)
                
        except Exception as e:
            app.logger.error(f"Error retrieving devices: {e}")
            return jsonify({'error': str(e)}), 500



@app.route('/livelyageing/add_device', methods=['POST'])
@login_required
def add_device():
    """
    Add a new device for the logged-in admin.
    """
    try:
        email_address = request.form['emailAddress']
        admin_user_id = int(current_user.id)
        
        with ConnectionManager() as conn:
            device_repo = DeviceRepository(conn)
            
            # Check if device already exists
            existing = device_repo.get_by_email(email_address)
            if existing:
                flash_translated('flash.device_already_exists', 'warning')
                return redirect(url_for('home'))
            
            # Create new device
            device_id = device_repo.create(
                admin_user_id=admin_user_id,
                email_address=email_address
            )
            
            if device_id:
                flash_translated('flash.device_added_successfully', 'success')
            else:
                flash_translated('flash.device_add_failed', 'danger')
                
    except Exception as e:
        app.logger.error(f"Error adding device: {e}")
        flash_translated('flash.error_occurred', 'danger')
    
    return redirect(url_for('home'))


@app.route('/livelyageing/update_devices_info')
@login_required
def update_devices_info():
    """
    Fetch device information from Fitbit and update database.
    This retrieves device type and last sync time automatically.
    """

    with ConnectionManager() as conn:
        device_repo = DeviceRepository(conn)
        devices = device_repo.get_all_authorized()

        errors = []
        for device in devices:

            try:
                access_token, _ = device_repo.get_tokens(device.id)
                device_data = get_device_info(access_token)

                device_result = device_repo.update_device_type(device.id, device_data['deviceVersion'])
                last_sync_result = device_repo.update_last_synch(device.id, device_data['lastSyncTime'])

                if not device_result or not last_sync_result:
                    app.logger.error(f"Error while updating info for device {device.id} linked to {device.email_address}")
                    errors.append(device.email_address)
                else:
                    app.logger.info(f"Device Info successfully updated for device {device.id} linked to {device.email_address}")

            except Exception as e:
                app.logger.error(f"Error retrieving device info or device {device.id} linked to {device.email_address}: {e}")
                errors.append(device.email_address)

        if len(errors) > 0:
            flash_translated('flash.devices_info_update_error', 'danger', devices=', '.join(errors))
        else:
            flash_translated('flash.devices_info_update_success', 'success')

    return redirect(url_for('home'))


@app.route('/livelyageing/send_auth_email', methods=['POST'])
@login_required
def send_auth_email():
    """Generate authorization url and send it by email"""
    device_id = request.form.get('deviceIdAuth')
    email_address = request.form.get('emailAddressAuth')
    if not email_address:
        flash_translated('flash.select_device', 'danger')
        return redirect(url_for('home'))

    # Generate code_verifier and store it temporarily with email as key
    code_verifier = generate_code_verifier()

    # Create state that includes email (encoded for security)
    state_data = {
        'email_address': email_address,
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

    if send_email(email_address, email_subject, email_html, email_text):
        # Store code_verifier in database or cache with state as key
        
        with ConnectionManager() as conn:
            auth_repo = AuthorizationRepository(conn)
            auth_repo.store_pending_auth(device_id, state, code_verifier)
        return render_template('auth_email_sent_confirmation.html', email_address=email_address)
    else:
        flash_translated('flash.device_send_error', 'danger')
        return redirect(url_for('home'))


# Callback to handle authorization confirmation
@app.route('/livelyageing/callback')
def callback():
    """
    Handle the callback from Fitbit after the user authorizes the app.
    """
    app.logger.info("Callback route accessed")


    code = request.args.get('code')
    state = request.args.get('state')

    if not code or not state:
        app.logger.error("Missing code or state parameter")
        flash_translated('flash.missing_auth_info', 'danger')
        return redirect(url_for('home'))

    # Decode state to get email
    try:
        state_data = json.loads(base64.urlsafe_b64decode(state.encode()).decode())
        email_address = state_data.get('email_address')
    except Exception as e:
        app.logger.error(f"Invalid state parameter: {e}")
        flash_translated('flash.invalid_auth_link', 'danger')
        return redirect(url_for('home'))

    if not email_address:
        app.logger.error("No email found in state")
        flash_translated('flash.invalid_auth_link', 'danger')
        return redirect(url_for('home'))

    with ConnectionManager() as conn:
        try:
            auth_repo = AuthorizationRepository(conn)
            # Retrieve code_verifier from database
            pending_auth = auth_repo.get_by_state(state)

            if not pending_auth:
                app.logger.error("No pending authorization found or expired")
                flash_translated('flash.auth_link_expired', 'danger')
                return redirect(url_for('home'))

            code_verifier = pending_auth['code_verifier']

            # Get tokens from Fitbit
            access_token, refresh_token = get_tokens(code, code_verifier)
            if not access_token or not refresh_token:
                raise Exception("Could not retrieve Fitbit tokens.")

            device_data = get_device_info(access_token)

            device_repo = DeviceRepository(conn)

            device = device_repo.get_by_email(email_address)

            device_repo.update_tokens(device.id, access_token, refresh_token)
            app.logger.info(f"Tokens updated for the device {device.id}.")

            device_repo.update_status(device.id, 'authorized')
            app.logger.info(f"Authorization status updated for the device {device.id}.")

            auth_repo.delete_by_state(state)
            app.logger.info(f"Deleted prending request.")

            device_repo.update_device_type(device.id, device_data['deviceVersion'])
            device_repo.update_last_synch(device.id, device_data['lastSyncTime'])

            return render_template('auth_confirmation.html',
                                     email_address=email_address,
                                     success=True,
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
    device_id = request.form.get('DeactivateId')

    with ConnectionManager() as conn:
        device_repo = DeviceRepository(conn)
        device_repo.update_status(device_id, 'non_active')

        app.logger.info(f"Device {device_id} deactivated.")

    return redirect(url_for('home'))


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


def get_text(key):
    """Get the translation for a key in the current language."""
    lang = str(get_locale())
    # Split the key by dots to access nested dictionaries
    keys = key.split('.')
    value = TRANSLATIONS.get(lang, {}).get(keys[0], {})
    for k in keys[1:]:
        value = value.get(k, '')
    return value if value else key

def translate_text(key):
    """Custom translation function that uses TRANSLATIONS dictionary."""
    lang = str(get_locale())
    
    if lang not in TRANSLATIONS:
        return key
    
    translations = TRANSLATIONS[lang]
    
    # Helper function to recursively search in a dictionary
    def search_in_dict(d, search_key, case_insensitive=False):
        """Search for a key in a nested dictionary."""
        if isinstance(d, dict):
            for k, v in d.items():
                if isinstance(v, dict):
                    result = search_in_dict(v, search_key, case_insensitive)
                    if result is not None:
                        return result
                elif isinstance(v, str):
                    compare_key = k.lower() if case_insensitive else k
                    compare_search = search_key.lower() if case_insensitive else search_key
                    if compare_key == compare_search:
                        return v
        return None
    
    # Strategy 1: Try with dots first (for nested keys like 'flash.welcome_user' or 'common.welcome')
    if '.' in key:
        parts = key.split('.')
        value = translations
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part) or value.get(part.lower())
            else:
                value = None
                break
        if isinstance(value, str):
            return value
    
    # Strategy 2: Try exact key match in all sections (case-sensitive)
    result = search_in_dict(translations, key, case_insensitive=False)
    if result:
        return result
    
    # Strategy 3: Try case-insensitive match
    result = search_in_dict(translations, key, case_insensitive=True)
    if result:
        return result
    
    # Strategy 4: Try with spaces replaced by underscores
    key_underscore = key.replace(' ', '_').lower()
    result = search_in_dict(translations, key_underscore, case_insensitive=True)
    if result:
        return result
    
    # Strategy 5: Try lowercase version
    key_lower = key.lower()
    result = search_in_dict(translations, key_lower, case_insensitive=True)
    if result:
        return result
    
    # Fallback: try Flask-Babel's translation
    try:
        from flask_babel import gettext as babel_gettext
        translated = babel_gettext(key)
        if translated != key:
            return translated
    except:
        pass
    
    # Last resort: return the key itself
    return key

def flash_translated(message_key, category='info', **kwargs):
    """Flash a translated message. Supports format strings with kwargs."""
    translated = translate_text(message_key)
    # Replace placeholders if kwargs are provided
    if kwargs:
        try:
            translated = translated.format(**kwargs)
        except (KeyError, ValueError):
            # If formatting fails, return the translated message as-is
            pass
    flash(translated, category)

@app.context_processor
def utility_processor():
    """Make translation function and static URL function available in templates."""
    def static_url(filename):
        """Generate full URL for static files."""
        # Use the complete path including /livelyageing prefix
        return url_for('static', filename=filename)
    
    # Override _() to use our custom translation function
    # This wrapper handles both Flask-Babel's gettext and our custom translation
    class CustomGettextWrapper:
        """Wrapper class that handles Flask-Babel gettext calls with parameters"""
        def __call__(self, message, **kwargs):
            """Handle gettext calls with keyword arguments"""
            translated = translate_text(message)
            if kwargs:
                try:
                    # Use % formatting for Flask-Babel syntax %(variable)s
                    translated = translated % kwargs
                except (KeyError, ValueError, TypeError):
                    # If % formatting fails, try .format() for {variable} syntax
                    try:
                        translated = translated.format(**kwargs)
                    except (KeyError, ValueError):
                        # If both fail, return translated message as-is
                        pass
            return translated
    
    custom_gettext = CustomGettextWrapper()
    
    return {
        'get_text': get_text,
        '_': custom_gettext,  # Override Flask-Babel's _() with our custom function
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

# @app.route('/livelyageing/refresh_data', methods=['POST'])
# @login_required
# def refresh_data():
#     """
#     Refresh Fitbit data for all users.
#     """
#     try:
#         # Get all unique emails from the database
#         db = DatabaseManager()
#         if not db.connect():
#             return jsonify({'error': 'Database connection error'}), 500

#         try:
#             emails = db.execute_query("SELECT DISTINCT email FROM users")
#         finally:
#             db.close()

#         # Process each email to fetch new data
#         from fitbit import process_emails
#         # from fitbit_intraday import process_emails as process_intraday_emails

#         # Process daily data
#         process_emails(emails)
#         # Process intraday data
#         # process_intraday_emails(emails)

#         return jsonify({'success': True})
#     except Exception as e:
#         app.logger.error(f"Error refreshing data: {e}")
#         return jsonify({'error': str(e)}), 500



@app.route("/livelyageing/device_details/<int:device_id>")
@login_required
def device_details(device_id):
    device = get_device(device_id)  # ← tuo metodo
    metrics = get_device_metrics(device_id)
        # Get all unique emails from the database
        

    device = {
                        "last_sync": "",
                        "last_daily_summary": "",
                        "last_intraday": "",
                    
            }

    metrics = {
        "steps": {
            "value": 7981,
            "history": [5000, 7000, 6500, 8000, 7800]
        },
        "distance": {
            "value": 5.3,
            "history": [3.2, 4.1, 4.8, 5.0, 5.3]
        },
        "elevation": {
            "value": 78,
            "history": [40,50,60,80,78]
        },
        "floors": {
            "value": 12,
            "history": [10,12,9,15,12]
        },
        "heart_rate": {
            "value": 75,
            "range": "(62–110 bpm)",
            "history": [70,72,75,78,75]
        }
    }

    medical_alerts = [
        "Battito cardiaco irregolare rilevato ieri",
        "Pausa prolungata senza attività"
    ]

    activity_alerts = [
        "Passi troppo bassi per 3 giorni consecutivi"
    ]
    alerts = [
        "Missing intraday data for 22 Apr",
        "Sync gap > 24h detected"
    ]

    return render_template(
        "device_details.html",
        device=device,
        metrics=metrics,
        alerts=alerts,
        medical_alerts=medical_alerts,
        activity_alerts=activity_alerts
    )


# @app.route('/livelyageing/api/daily_summary')
# @login_required
# def get_daily_summary():
#     """
#     Gets the most recent daily summary of the current user.
#     """
#     try:
#         db = DatabaseManager()
#         if not db.connect():
#             return jsonify({'error': 'Database connection failed'}), 500
        
#         try:
#             user_id = db.get_user_id_by_email(current_user.email)
#             if not user_id:
#                 return jsonify({'error': 'User not found'}), 404

#             # Get the most recent summary
#             summaries = db.get_daily_summaries(
#                 device_id=user_id,
#                 start_date=datetime.now() - timedelta(days=1),
#                 end_date=datetime.now()
#             )
#         finally:
#             db.close()

#         if not summaries:
#             return jsonify({'error': 'No data available.'}), 404

#         latest_summary = summaries[-1]

#         return jsonify({
#             'steps': latest_summary[3],
#             'heart_rate': latest_summary[4],
#             'sleep_minutes': latest_summary[5],
#             'calories': latest_summary[6],
#             'distance': latest_summary[7],
#             'floors': latest_summary[8],
#             'elevation': latest_summary[9],
#             'active_minutes': latest_summary[10],
#             'sedentary_minutes': latest_summary[11],
#             'nutrition_calories': latest_summary[12],
#             'water': latest_summary[13],
#             'weight': latest_summary[14],
#             'bmi': latest_summary[15],
#             'fat': latest_summary[16],
#             'oxygen_saturation': latest_summary[17],
#             'respiratory_rate': latest_summary[18],
#             'temperature': latest_summary[19]
#         })

#     except Exception as e:
#         app.logger.error(f"Error getting the daily summary.: {str(e)}")
#         return jsonify({'error': 'Internal server error.'}), 500


# @app.route('/livelyageing/api/user/<int:user_id>/daily_summary')
# @login_required
# def api_user_daily_summary(user_id):
#     """
#     Returns the daily summary for a user and a date (today by default)
#     """
#     date_str = request.args.get('date')
#     if date_str:
#         try:
#             date = datetime.strptime(date_str, "%Y-%m-%d").date()
#         except Exception:
#             return jsonify({'error': 'Invalid date format'}), 400
#     else:
#         date = datetime.now().date()
#     db = DatabaseManager()
#     if not db.connect():
#         return jsonify({'error': 'DB error'}), 500
#     try:
#         summary = db.execute_query(
#             """
#             SELECT
#                 date,
#                 steps,
#                 heart_rate,
#                 sleep_minutes,
#                 calories,
#                 distance,
#                 floors,
#                 elevation,
#                 active_minutes,
#                 sedentary_minutes,
#                 nutrition_calories,
#                 water,
#                 weight,
#                 bmi,
#                 fat,
#                 oxygen_saturation,
#                 respiratory_rate,
#                 temperature
#             FROM daily_summaries
#             WHERE user_id = %s AND date = %s
#             """, (user_id, date)
#         )
#         if not summary:
#             return jsonify({'error': 'No hay datos para ese día'}), 404

#         # Mapear los campos a nombres legibles
#         columns = [desc[0] for desc in db.cursor.description]
#         summary_dict = dict(zip(columns, summary[0]))

#         # Calcular valores adicionales
#         if summary_dict.get('sleep_minutes'):
#             summary_dict['sleep_hours'] = round(summary_dict['sleep_minutes'] / 60, 1)
#         if summary_dict.get('sedentary_minutes'):
#             summary_dict['sedentary_hours'] = round(summary_dict['sedentary_minutes'] / 60, 1)
#         return jsonify({'summary': summary_dict})
#     finally:
#         db.close()

# @app.route('/livelyageing/api/user/<int:user_id>/intraday')
# @login_required
# def api_user_intraday(user_id):
#     """
#     Returns intraday data for the users together with date and metric type.
#     """
#     date_str = request.args.get('date')
#     metric_type = request.args.get('type')
#     if not metric_type:
#         return jsonify({'error': 'The metric type is missing.'}), 400
#     if date_str:
#         try:
#             date = datetime.strptime(date_str, "%Y-%m-%d").date()
#         except Exception:
#             return jsonify({'error': 'Invalid date format'}), 400
#     else:
#         date = datetime.now().date()

#     db = DatabaseManager()
#     if not db.connect():
#         return jsonify({'error': 'DB error'}), 500
#     try:
#         start_time = datetime.combine(date, datetime.min.time())
#         end_time = datetime.combine(date, datetime.max.time())
#         data = db.execute_query(
#             f"""
#             SELECT time, {metric_type}
#             FROM intraday_metrics
#             WHERE user_id = %s
#             AND time BETWEEN %s AND %s
#             ORDER BY time
#             """, (user_id, start_time, end_time)
#         )

#         # for row in data:
#         #     print(row)
#         #     print(len(row))


#         return jsonify({
#             'intraday': [
#                 {
#                     'time': row[0].strftime('%H:%M'),
#                     'value': float(row[1] if row[1] is not None else 0)
#                 } for row in data
#             ]
#         })
#     finally:
#         db.close()

# @app.route('/livelyageing/api/user/<int:user_id>/weekly_summary')
# @login_required
# def api_user_weekly_summary(user_id):
#     """
#     Returns the daily summaries from the last 7 days for the user.
#     """
#     db = DatabaseManager()
#     if not db.connect():
#         return jsonify({'error': 'DB error'}), 500
#     try:
#         end_date = datetime.now().date()
#         start_date = end_date - timedelta(days=6)
#         data = db.execute_query(
#             """
#             SELECT
#                 date,
#                 steps,
#                 heart_rate,
#                 sleep_minutes,
#                 calories,
#                 sedentary_minutes,
#                 active_minutes,
#                 distance,
#                 floors,
#                 elevation,
#                 nutrition_calories,
#                 water,
#                 weight,
#                 bmi,
#                 fat,
#                 oxygen_saturation,
#                 respiratory_rate,
#                 temperature
#             FROM daily_summaries
#             WHERE user_id = %s
#             AND date BETWEEN %s AND %s
#             ORDER BY date DESC
#             """, (user_id, start_date, end_date)
#         )
#         return jsonify({
#             'weekly': [
#                 {
#                     'date': row[0].strftime('%d/%m'),
#                     'steps': row[1],
#                     'heart_rate': row[2],
#                     'sleep_hours': round(row[3] / 60, 1) if row[3] else None,
#                     'calories': row[4],
#                     'sedentary_hours': round(row[5] / 60, 1) if row[5] else None,
#                     'active_minutes': row[6],
#                     'distance': row[7],
#                     'floors': row[8],
#                     'elevation': row[9],
#                     'nutrition_calories': row[10],
#                     'water': row[11],
#                     'weight': row[12],
#                     'bmi': row[13],
#                     'fat': row[14],
#                     'oxygen_saturation': row[15],
#                     'respiratory_rate': row[16],
#                     'temperature': row[17]
#                 } for row in data
#             ]
#         })
#     finally:
#         db.close()

# @app.route('/livelyageing/api/user/<int:user_id>/alerts')
# @login_required
# def api_user_alerts(user_id):
#     """
#     Return the alerts from the last 7 days for the user
#     """
#     db = DatabaseManager()
#     if not db.connect():
#         return jsonify({'error': 'DB error'}), 500
#     try:
#         since = datetime.now() - timedelta(days=7)
#         data = db.execute_query(
#             """
#             SELECT
#                 alert_time,
#                 alert_type,
#                 priority,
#                 triggering_value,
#                 threshold_value,
#                 details,
#                 acknowledged
#             FROM alerts
#             WHERE user_id = %s
#             AND alert_time >= %s
#             ORDER BY alert_time DESC
#             """, (user_id, since)
#         )

#         return jsonify({
#             'alerts': [
#                 {
#                     'alert_time': row[0].strftime('%d/%m %H:%M'),
#                     'type': row[1],
#                     'priority': row[2],
#                     'triggering_value': row[3],
#                     'threshold_value': row[4],
#                     'details': row[5],
#                     'acknowledged': row[6]
#                 } for row in data
#             ]
#         })
#     finally:
#         db.close()

# @app.route('/livelyageing/dashboard/alerts/export')
# @login_required
# def export_alerts():
#     import csv
#     from io import StringIO
#     db = DatabaseManager()
#     if not db.connect():
#         return "Database connection error", 500
#     try:
#         # Get filters the same way as in alerts_dashboard
#         date_from = request.args.get('date_from')
#         date_to = request.args.get('date_to')
#         priority = request.args.get('priority')
#         acknowledged = request.args.get('acknowledged')
#         user_query = request.args.get('user_query')

#         # Build the base query.
#         query = """
#             SELECT
#                 a.alert_time,
#                 u.name AS user_name,
#                 u.email AS user_email,
#                 a.alert_type,
#                 a.priority,
#                 a.triggering_value,
#                 a.threshold_value,
#                 a.details,
#                 a.acknowledged
#             FROM alerts a
#             JOIN users u ON a.user_id = u.id
#             WHERE 1=1
#         """
#         params = []
#         if date_from:
#             query += " AND a.alert_time >= %s"
#             params.append(f"{date_from} 00:00:00")
#         if date_to:
#             query += " AND a.alert_time <= %s"
#             params.append(f"{date_to} 23:59:59")
#         if priority:
#             query += " AND a.priority = %s"
#             params.append(priority)
#         if acknowledged is not None and acknowledged != '':
#             query += " AND a.acknowledged = %s"
#             params.append(acknowledged == 'true')
#         if user_query:
#             query += " AND (LOWER(u.name) LIKE LOWER(%s) OR LOWER(u.email) LIKE LOWER(%s))"
#             search_term = f"%{user_query}%"
#             params.extend([search_term, search_term])
#         query += " ORDER BY a.alert_time DESC"
#         alerts = db.execute_query(query, params)

#         # Create a CSV with UTF-8 BOM for Excel compatibility.
#         si = StringIO()
#         cw = csv.writer(si)
#         cw.writerow(["Date", "User", "Email", "alertType", "Priority", "Trigger Value", "Threshold", "Details", "Acknowledged"])
#         for a in alerts:
#             cw.writerow([
#                 a[0].strftime('%Y-%m-%d %H:%M'),
#                 a[1], a[2], a[3], a[4], a[5], a[6], a[7], "Sí" if a[8] else "No"
#             ])
#         output = '\ufeff' + si.getvalue()  # Add BOM UTF-8
#         si.close()
#         date = datetime.now().strftime('%Y%m%d')
#         return Response(
#             output,
#             mimetype="text/csv; charset=utf-8",
#             headers={"Content-Disposition": f"attachment;filename=alertas_{date}.csv"}
#         )
#     finally:
#         db.close()

# @app.route('/livelyageing/user/<int:user_id>/export_alerts')
# @login_required
# def export_user_alerts(user_id):
#     import csv
#     from io import StringIO
#     db = DatabaseManager()
#     if not db.connect():
#         return "Database connection error", 500
#     try:
#         since = datetime.now() - timedelta(days=7)
#         query = """
#             SELECT
#                 a.alert_time,
#                 u.name AS user_name,
#                 u.email AS user_email,
#                 a.alert_type,
#                 a.priority,
#                 a.triggering_value,
#                 a.threshold_value,
#                 a.details,
#                 a.acknowledged
#             FROM alerts a
#             JOIN users u ON a.user_id = u.id
#             WHERE a.user_id = %s AND a.alert_time >= %s
#             ORDER BY a.alert_time DESC
#         """
#         alerts = db.execute_query(query, (user_id, since))
#         si = StringIO()
#         cw = csv.writer(si)
#         cw.writerow(["Date/Hour", "User", "Email", "Alert type", "Priority", "Trigger value", "Threshold", "Details", "Acknowledged"])
#         for a in alerts:
#             cw.writerow([
#                 a[0].strftime('%Y-%m-%d %H:%M'),
#                 a[1], a[2], a[3], a[4], a[5], a[6], a[7], "Sí" if a[8] else "No"
#             ])
#         output = '\ufeff' + si.getvalue()
#         si.close()
#         date = datetime.now().strftime('%Y%m%d')
#         return Response(
#             output,
#             mimetype="text/csv; charset=utf-8",
#             headers={"Content-Disposition": f"attachment;filename=alerts_user_{user_id}_{date}.csv"}
#         )
#     finally:
#         db.close()

# @app.route('/livelyageing/user/<int:user_id>/export_intraday')
# @login_required
# def export_user_intraday(user_id):
#     import csv
#     from io import StringIO
#     db = DatabaseManager()
#     if not db.connect():
#         return "Database connection error", 500
#     try:
#         # Obtain selected dates and metrics
#         dates = request.args.getlist('dates')
#         metrics = request.args.getlist('metrics')
#         if not dates or not metrics:
#             return "You must select at least one date and one metric.", 400

#         # Set query
#         rows = []
#         for date_str in dates:
#             for metric in metrics:
#                 start_time = datetime.strptime(date_str, "%Y-%m-%d")
#                 end_time = start_time + timedelta(days=1)
#                 query = """
#                     SELECT time, type, value
#                     FROM intraday_metrics
#                     WHERE user_id = %s AND type = %s AND time >= %s AND time < %s
#                     ORDER BY time
#                 """
#                 data = db.execute_query(query, (user_id, metric, start_time, end_time))
#                 for row in data:
#                     rows.append((row[0].date().strftime('%Y-%m-%d'), row[0].strftime('%H:%M'), row[1], row[2]))
#         # Crear CSV
#         si = StringIO()
#         cw = csv.writer(si)
#         cw.writerow(["Date", "Hour", "Metric", "Value"])
#         for r in rows:
#             cw.writerow(r)
#         output = '\ufeff' + si.getvalue()
#         si.close()
#         date = datetime.now().strftime('%Y%m%d')
#         return Response(
#             output,
#             mimetype="text/csv; charset=utf-8",
#             headers={"Content-Disposition": f"attachment;filename=intraday_user_{user_id}_{date}.csv"}
#         )
#     finally:
#         db.close()


# Run the Flask app
if __name__ == '__main__':
    # app.run(host=HOST, port=PORT, debug=DEBUG)
    app.run(debug=True)
