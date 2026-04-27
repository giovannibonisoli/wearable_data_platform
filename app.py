from logging.handlers import RotatingFileHandler
from functools import wraps
from flask import Flask, logging, render_template, request, redirect, session, url_for, flash, g, jsonify, Response
from rich import _console

from flask_login import current_user, login_user, logout_user, login_required
from flask_login import LoginManager, UserMixin
from datetime import datetime, timedelta, timezone, time
from flask_babel import Babel, get_locale, gettext

from database import ConnectionManager
from extensions import db, migrate
from services import DeviceService, DeviceStatisticsService, StaffUserService, CareProviderService
from services.result_enums import (
    ChangePasswordResult,
    AddDeviceResult,
    SendAuthEmailResult,
    AuthGrantResult,
    AdminCreateStaffUserResult,
    AdminResetPasswordResult,
    AdminDeactivateStaffUserResult,
    AdminCreateCareProviderResult,
    AdminRenameCareProviderResult
)

import os
import logging
from urllib.parse import quote_plus


# Initialize Flask app
app = Flask(__name__,
           static_url_path='/livelyageing/static',  # Prefix for static files with livelyageing
           static_folder='static')  # Directory where static files are stored


app.secret_key = os.getenv('SECRET_KEY')

_sqlalchemy_uri = os.getenv("SQLALCHEMY_DATABASE_URI")
if not _sqlalchemy_uri:
    _db_user = os.getenv("DB_USER")
    _db_password = os.getenv("DB_PASSWORD")
    _db_host = os.getenv("DB_HOST")
    _db_port = os.getenv("DB_PORT")
    _db_name = os.getenv("DB_NAME")
    if all((_db_user, _db_password, _db_host, _db_port, _db_name)):
        _sqlalchemy_uri = (
            f"postgresql+psycopg2://{_db_user}:{quote_plus(_db_password)}"
            f"@{_db_host}:{_db_port}/{_db_name}"
        )
        app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
            "connect_args": {"sslmode": os.getenv("DB_SSLMODE", "require")}
        }
if _sqlalchemy_uri:
    app.config["SQLALCHEMY_DATABASE_URI"] = _sqlalchemy_uri
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db.init_app(app)
migrate.init_app(app, db)

import database.orm_models  # noqa: F401  — register models for Flask-Migrate

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
    'en': 'English',
    'it': 'Italiano',
    'es': 'Español'
}

DEFAULT_LANGUAGE = 'it'

# Initialize Babel
babel = Babel()

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
    def home_url():
        if session.get('role') == 'ADMIN':
            return url_for('admin_care_providers')
        return url_for('home')

    return {
        'LANGUAGES': LANGUAGES,
        'get_locale': lambda: str(get_locale()),
        'current_language': lambda: session.get('language', DEFAULT_LANGUAGE),
        'home_url': home_url,
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


def admin_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for('login'))
        if session.get('role') != 'ADMIN':
            flash(gettext('Access denied.'), 'danger')
            return redirect(url_for('home'))
        return view(*args, **kwargs)
    return wrapped


def staff_user_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for('login'))
        if session.get('role') != 'STAFF':
            flash(gettext('This area is only available to staff user accounts.'), 'info')
            return redirect(url_for('admin_care_providers'))
        return view(*args, **kwargs)
    return wrapped


@app.route('/livelyageing/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        if session.get('role') == 'ADMIN':
            return redirect(url_for('admin_care_providers'))
        return redirect(url_for('home'))

    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        with ConnectionManager() as conn:
            staff_user_service = StaffUserService(conn)
            user_data = staff_user_service.check_user(username, password)

            if user_data:
                user = User(user_data['id'])
                login_user(user)
                    
                # Store user info in session for easy access
                session['user_id'] = user_data['id']
                session['username'] = user_data['username']
                session['role'] = user_data['role']
                    
                name = user_data["full_name"] or username
                flash(gettext('Welcome, %(name)s!', name=name), 'success')
                if user_data['role'] == 'ADMIN':
                    return redirect(url_for('admin_care_providers'))
                return redirect(url_for('home'))
            else:
                flash(gettext('Incorrect username or password.'), 'danger')
    
    return render_template('login.html')



# Logout path
@app.route('/livelyageing/logout')
@login_required
def logout():
    logout_user()
    for k in ('user_id', 'username', 'role'):
        session.pop(k, None)
    return redirect(url_for('login'))


@app.before_request
def require_login():
    print(f"🔍 Requested endpoint: {request.endpoint}")  # ← Debug
    print(f"🔐 Utente autenticato: {current_user.is_authenticated}")  # ← Debug

    public_endpoints = ['login', 'callback', 'static']

    if not current_user.is_authenticated and request.endpoint not in public_endpoints:
        print(f"❌ Bloked! Recirect to login")  # ← Debug
        return redirect(url_for('login'))

    if current_user.is_authenticated and session.get('role') is None:
        with ConnectionManager() as conn:
            staff_user_service = StaffUserService(conn)
            role = staff_user_service.get_role_for_user(int(current_user.id))
        if role:
            session['role'] = role
        else:
            logout_user()
            for k in ('user_id', 'username', 'role'):
                session.pop(k, None)
            return redirect(url_for('login'))

    print(f"✅ Access allowed")

# Route: Root URL redirect
@app.route('/')
def root():
    """
    Redirect from root URL to the home page or admin dashboard.
    """
    if current_user.is_authenticated:
        if session.get('role') == 'ADMIN':
            return redirect(url_for('admin_care_providers'))
        return redirect(url_for('home'))
    return redirect(url_for('login'))

# Route: Homepage
@app.route('/livelyageing/')
@login_required
def index():
    """
    Redirect to home page or admin dashboard.
    """
    if session.get('role') == 'ADMIN':
        return redirect(url_for('admin_care_providers'))
    return redirect(url_for('home'))


@app.route('/livelyageing/user_profile_info')
@login_required
def user_profile_info():
    try:
        with ConnectionManager() as conn:
            staff_user_service = StaffUserService(conn)
            user_id = int(current_user.id)
            user_info = staff_user_service.get_user_info(user_id)
             
            return render_template('user_profile_info.html', user=user_info)
    except Exception as e:
        app.logger.error(f"Error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/livelyageing/change_password', methods=['POST'])
@login_required
def change_password():
    
    current_password = request.form['current_password']
    new_password = request.form['new_password']
    confirm_password = request.form['confirm_password']

    if new_password == confirm_password:
        user_id = int(current_user.id)

        if len(new_password) >= 8:
            with ConnectionManager() as conn:
                staff_user_service = StaffUserService(conn)
                result = staff_user_service.check_and_change_password(user_id, current_password, new_password)

                if result == ChangePasswordResult.SUCCESS:
                    flash(gettext('Password changed successfully.'), 'success')
                elif result == ChangePasswordResult.NO_CURRENT_PASSWORD:
                    flash(gettext('The current password is wrong.'), 'danger')
                else:
                    flash(gettext('Password change failed.'), 'danger')
        else:
            flash(gettext('The new password must be at least 8 characters.'), 'danger')
    else:
        flash(gettext('Passwords do not match.'), 'danger')
    
    return redirect(url_for('user_profile_info'))


@app.route('/livelyageing/home')
@login_required
@staff_user_required
def home():
    try:
        with ConnectionManager() as conn:
            staff_user_service = StaffUserService(conn)
            care_provider_service = CareProviderService(conn)
            device_service = DeviceService(conn)
 
            user_id = int(current_user.id)
            user_info = staff_user_service.get_user_info(user_id)
 
            care_provider_id = user_info['care_provider_id']
            care_provider = care_provider_service.get_care_provider_by_id(care_provider_id)
 
            devices = device_service.get_devices_by_care_provider(care_provider_id)
            device_count = len(devices) if devices else 0
 
        return render_template(
            "home.html",
            user_info=user_info,
            care_provider=care_provider,
            device_count=device_count
        )
    except Exception as e:
        app.logger.error(f"Error loading home page: {e}")
        flash(gettext('Error loading the home page.'), 'danger')
        return redirect(url_for('login'))


import json
import os

VIDEO_TUTORIALS_FILE = os.path.join(os.path.dirname(__file__), 'video_tutorials.json')


def load_video_tutorials(locale=None, context=None):
    """Load video tutorials from external JSON file and localize descriptions."""
    try:
        with open(VIDEO_TUTORIALS_FILE, 'r', encoding='utf-8') as f:
            videos = json.load(f)
    except FileNotFoundError:
        app.logger.warning(f"Video tutorials file not found: {VIDEO_TUTORIALS_FILE}")
        return {}
    except json.JSONDecodeError as e:
        app.logger.error(f"Invalid JSON in video tutorials file: {e}")
        return {}

    if locale is None:
        locale = get_locale()

    localized_videos = {}
    for video in videos:
        video_context = video.get('context', '')
        if context and video_context != context and video_context:
            continue
        topic = video.get('topic')
        if topic:
            localized_videos[topic] = {
                'id': video['id'],
                'title': video.get('title', {}).get(locale, video.get('title', {}).get('en', '')),
                'description': video.get('description', {}).get(locale, video.get('description', {}).get('en', '')),
                'youtube_id': video['youtube_id'],
                'context': video_context,
            }
    return localized_videos


@app.route('/livelyageing/video_tutorials')
@login_required
@staff_user_required
def video_tutorials():
    """
    Display list of video tutorials for staff users.
    The list is loaded from video_tutorials.json file.
    """
    videos_dict = load_video_tutorials(get_locale())
    videos = list(videos_dict.values())
    return render_template(
        'video_tutorials.html',
        videos=videos,
    )


@app.route('/livelyageing/video_tutorials/<int:video_id>')
@login_required
@staff_user_required
def video_tutorial_detail(video_id):
    """
    Display a single video tutorial.
    """
    videos_dict = load_video_tutorials(get_locale())
    videos = list(videos_dict.values())
    video = next((v for v in videos if v['id'] == video_id), None)
    
    if not video:
        flash(gettext('Video not found.'), 'warning')
        return redirect(url_for('video_tutorials'))
    
    return render_template(
        'video_tutorial_detail.html',
        video=video,
    )


@app.route('/livelyageing/device_list')
@login_required
@staff_user_required
def device_list():
    """
    Display all devices for the logged-in user.
    """

    with ConnectionManager() as conn:
        device_service = DeviceService(conn)
        device_stats_service = DeviceStatisticsService(conn)
        staff_user_service = StaffUserService(conn)

        try:
            user_id = int(current_user.id)
            user_info = staff_user_service.get_user_info(user_id)
            devices_data = device_service.get_devices_by_care_provider(user_info['care_provider_id'])
            
            
            final_devices_data = []
            for device_data in devices_data:

                data_reception_status = 'no_data'
                data_reception_details = {}
                device_usage_details = {}

                if device_data["auth_status"] == 'INSERTED' and device_data["is_pending_auth"]:
                    device_data["auth_status"] = "pending_auth_request"
                    
                elif device_data["auth_status"] == 'AUTHORIZED':
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
                
            return render_template('device_list.html', devices=final_devices_data, tutorial_videos=load_video_tutorials(get_locale(), 'device_list'))
                
        except Exception as e:
            app.logger.error(f"Error retrieving devices: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500



@app.route('/livelyageing/add_device', methods=['POST'])
@login_required
@staff_user_required
def add_device():
    """
    Add a new device for the logged-in user.
    """
    try:
        
        email_address = request.form['emailAddress']
        
        with ConnectionManager() as conn:
            staff_user_service = StaffUserService(conn)
            device_service = DeviceService(conn)

            user_id = int(current_user.id)
            user_info = staff_user_service.get_user_info(user_id)
            care_provider_id = user_info['care_provider_id']
            
            result = device_service.add_new_device(care_provider_id, email_address)
            

            if result == AddDeviceResult.ALREADY_EXISTS:
                flash(gettext('This device is already registered.'), 'warning')
            elif result == AddDeviceResult.ADDED:
                flash(gettext('Device added successfully.'), 'success')
            else:
                flash(gettext('Error adding device.'), 'danger')
                
    except Exception as e:
        app.logger.error(f"Error adding device: {e}")
        flash(gettext('An error occurred.'), 'danger')
    
    return redirect(url_for('device_list'))


@app.route('/livelyageing/update_devices_info')
@login_required
@staff_user_required
def update_devices_info():
    """
    Fetch device information from Fitbit and update database.
    This retrieves device type and last sync time automatically.
    """

    with ConnectionManager() as conn:
        staff_user_service = StaffUserService(conn)
        device_service = DeviceService(conn)

        user_id = int(current_user.id)
        user_info = staff_user_service.get_user_info(user_id)
        care_provider_id = user_info['care_provider_id']

        errors = device_service.update_devices_info_by_care_provider(care_provider_id)

        if len(errors) > 0:
            app.logger.error(f"Error while updating info for devices linked to {', '.join(errors)}")
            flash(gettext('Error updating device info for: %(devices)s', devices=', '.join(errors)), 'danger')
        else:
            app.logger.error(f"Info regarding all the devices have been successfully updated")
            flash(gettext('Device information updated successfully for all devices.'), 'success')

    return redirect(url_for('device_list'))


@app.route('/livelyageing/send_auth_request', methods=['POST'])
@login_required
@staff_user_required
def send_auth_request():
    """Generate authorization url and send it by email"""
    device_id = request.form.get('deviceIdAuth')

    with ConnectionManager() as conn:
        device_service = DeviceService(conn)

        email_address, result = device_service.send_authorization_email(device_id)

        if result == SendAuthEmailResult.SUCCESS:
            app.logger.info(f"Authorization request successfully sent to {email_address} linked to device {device_id}")
            return render_template('auth_email_sent_confirmation.html', email_address=email_address)

        elif result == SendAuthEmailResult.EMAIL_SENDING_ERROR:
            app.logger.error(f"Error sending authorization request to {email_address} linked to device {device_id}")
            flash(gettext('Error sending device. Please try again.'), 'danger')
        else:
            app.logger.error(f"Error storing authorization request in db for {email_address} linked to device {device_id}")
            flash(gettext('Error storing authorization request.'), 'danger')

    return redirect(url_for('device_list'))


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
        flash(gettext('Error: Missing authorization information.'), 'danger')
        return redirect(url_for('device_list'))

    with ConnectionManager() as conn:
        try:
            device_service = DeviceService(conn)
            message, email_address = device_service.handle_authorization_grant(code, state)

            if message == AuthGrantResult.MISSING_AUTH_INFO:
                app.logger.error("No code or state found")
                flash(gettext('Error: Missing authorization information.'), 'danger')
                return redirect(url_for('device_list'))

            elif message == AuthGrantResult.EMAIL_NOT_FOUND:
                app.logger.error("No email found")
                flash(gettext('Email not found.'), 'danger')
                return redirect(url_for('device_list'))

            elif message == AuthGrantResult.INVALID_AUTH_LINK:
                app.logger.error("No pending authorization found or expired")
                flash(gettext('Error: Authorization link expired. Please request a new one.'), 'danger')
                return redirect(url_for('device_list'))

            elif message == AuthGrantResult.ERROR_RETRIEVE_TOKENS:
                app.logger.error("Error while retrieving tokens")
                flash(gettext('Error: Authorization link expired. Please request a new one.'), 'danger')
                return redirect(url_for('device_list'))

            elif message == AuthGrantResult.ERROR_STATE_UPDATE:
                app.logger.error("Error while updating state")
                flash(gettext('Error: Authorization link expired. Please request a new one.'), 'danger')
                return redirect(url_for('device_list'))

            else:
                app.logger.info("Authorization obtained!")
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


@app.route('/livelyageing/deactivate_device', methods=['POST'])
@login_required
@staff_user_required
def deactivate_device():
    """ Deactivate authorized device"""
    device_id = request.form.get('DeactivateId')

    with ConnectionManager() as conn:
        device_service = DeviceService(conn)
        device_service.deactivate_device(device_id)
        app.logger.info(f"Device {device_id} deactivated.")

    return redirect(url_for('device_list'))


@app.route('/livelyageing/rename_device_email/<int:device_id>', methods=['POST'])
@login_required
@staff_user_required
def rename_device_email(device_id):
    """Rename the email address of an inserted (not yet authorized) device."""
    new_email = (request.form.get('newEmailAddress') or '').strip()

    if not new_email:
        flash(gettext('Email address is required.'), 'danger')
        return redirect(url_for('device_list'))

    try:
        with ConnectionManager() as conn:
            device_service = DeviceService(conn)
            result = device_service.rename_device_email(device_id, new_email)

            if result:
                app.logger.info(f"Device {device_id} email renamed to {new_email}.")
                flash(gettext('Email address updated successfully.'), 'success')
            else:
                app.logger.error(f"Failed to rename email for device {device_id}.")
                flash(gettext('Error updating email address.'), 'danger')

    except Exception as e:
        app.logger.error(f"Error renaming device email: {e}")
        flash(gettext('An error occurred.'), 'danger')

    return redirect(url_for('device_list'))


@app.route('/livelyageing/admin/care_providers', methods=['GET'])
@login_required
@admin_required
def admin_care_providers():
    with ConnectionManager() as conn:
        care_provider_service = CareProviderService(conn)
        care_providers = care_provider_service.get_all_care_providers()

    return render_template('admin_care_providers.html', care_providers=care_providers)


@app.route('/livelyageing/admin/care_providers/add', methods=['POST'])
@login_required
@admin_required
def admin_add_care_provider():
    full_name = (request.form.get('full_name') or '').strip()

    with ConnectionManager() as conn:
        care_provider_service = CareProviderService(conn)
        result = care_provider_service.create_care_provider(full_name)

    if result == AdminCreateCareProviderResult.SUCCESS:
        flash(gettext('Care provider created.'), 'success')
    else:
        flash(gettext('Could not create care provider.'), 'danger')

    return redirect(url_for('admin_care_providers'))


@app.route('/livelyageing/admin/<int:care_provider_id>/rename', methods=['POST'])
@login_required
@admin_required
def admin_rename_care_provider(care_provider_id):
    full_name = (request.form.get('full_name') or '').strip()
 
    if not full_name:
        flash(gettext('Full name is required.'), 'danger')
        return redirect(url_for('admin_care_providers'))
 
    with ConnectionManager() as conn:
        care_provider_service = CareProviderService(conn)
        result = care_provider_service.rename_care_provider(care_provider_id, full_name)
 
    if result == AdminRenameCareProviderResult.SUCCESS:
        flash(gettext('Care provider renamed.'), 'success')
    elif result == AdminRenameCareProviderResult.NAME_EXISTS:
        flash(gettext('A care provider with that name already exists.'), 'warning')
    else:
        flash(gettext('Could not rename care provider.'), 'danger')
 
    return redirect(url_for('admin_care_providers'))


@app.route('/livelyageing/admin/<int:care_provider_id>/staff_users', methods=['GET'])
@login_required
@admin_required
def admin_staff_users(care_provider_id):
    with ConnectionManager() as conn:
        staff_user_service = StaffUserService(conn)
        staff_users = staff_user_service.get_staff_users_by_care_provider(care_provider_id)
 
    return render_template(
        'admin_staff_users.html',
        staff_users=staff_users,
        current_care_provider_id=care_provider_id,
    )

@app.route('/livelyageing/admin/<int:care_provider_id>/staff_users/add', methods=['POST'])
@login_required
@admin_required
def admin_add_staff_user(care_provider_id):
    username = (request.form.get('username') or '').strip()
    full_name = (request.form.get('full_name') or '').strip()
    password = request.form.get('password') or ''
    confirm = request.form.get('confirm_password') or ''
 
    if not username:
        flash(gettext('Username is required.'), 'danger')
        return redirect(url_for('admin_staff_users', care_provider_id=care_provider_id))
    if password != confirm:
        flash(gettext('Passwords do not match.'), 'danger')
        return redirect(url_for('admin_staff_users', care_provider_id=care_provider_id))
    if len(password) < 8:
        flash(gettext('The password must be at least 8 characters.'), 'danger')
        return redirect(url_for('admin_staff_users', care_provider_id=care_provider_id))
 
    with ConnectionManager() as conn:
        staff_user_service = StaffUserService(conn)
        result = staff_user_service.create_staff_user(username, full_name, password, care_provider_id)
 
    if result == AdminCreateStaffUserResult.SUCCESS:
        flash(gettext('Staff user created.'), 'success')
    elif result == AdminCreateStaffUserResult.USERNAME_EXISTS:
        flash(gettext('That username is already taken.'), 'warning')
    else:
        flash(gettext('Could not create staff user.'), 'danger')
 
    return redirect(url_for('admin_staff_users', care_provider_id=care_provider_id))


@app.route('/livelyageing/admin/<int:care_provider_id>/staff_users/<int:user_id>/reset_password', methods=['POST'])
@login_required
@admin_required
def admin_reset_staff_user_password(care_provider_id, user_id):
    new_password = request.form.get('new_password') or ''
    confirm = request.form.get('confirm_password') or ''
 
    if new_password != confirm:
        flash(gettext('Passwords do not match.'), 'danger')
        return redirect(url_for('admin_staff_users', care_provider_id=care_provider_id))
    if len(new_password) < 8:
        flash(gettext('The password must be at least 8 characters.'), 'danger')
        return redirect(url_for('admin_staff_users', care_provider_id=care_provider_id))
 
    admin_id = int(current_user.id)
    with ConnectionManager() as conn:
        staff_user_service = StaffUserService(conn)
        result = staff_user_service.admin_reset_staff_user_password(admin_id, user_id, new_password)
 
    if result == AdminResetPasswordResult.SUCCESS:
        flash(gettext('Password updated.'), 'success')
    elif result == AdminResetPasswordResult.NOT_FOUND:
        flash(gettext('User not found.'), 'danger')
    elif result == AdminResetPasswordResult.FORBIDDEN:
        flash(gettext('You cannot change that password.'), 'danger')
    else:
        flash(gettext('Password update failed.'), 'danger')
 
    return redirect(url_for('admin_staff_users', care_provider_id=care_provider_id))
    

@app.route('/livelyageing/admin/<int:care_provider_id>/staff_users/<int:user_id>/deactivate', methods=['POST'])
@login_required
@admin_required
def admin_deactivate_staff_user(care_provider_id, user_id):
    admin_id = int(current_user.id)
    with ConnectionManager() as conn:
        staff_user_service = StaffUserService(conn)
        result = staff_user_service.admin_deactivate_staff_user(admin_id, user_id)
 
    if result == AdminDeactivateStaffUserResult.SUCCESS:
        flash(gettext('Staff user deactivated.'), 'success')
    elif result == AdminDeactivateStaffUserResult.NOT_FOUND:
        flash(gettext('Staff user not found.'), 'danger')
    elif result == AdminDeactivateStaffUserResult.FORBIDDEN:
        flash(gettext('You cannot deactivate this staff user.'), 'danger')
    else:
        flash(gettext('Deactivation failed.'), 'danger')
 
    return redirect(url_for('admin_staff_users', care_provider_id=care_provider_id))


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


@app.context_processor
def utility_processor():
    """Make static URL function available in templates. Flask-Babel provides _ and gettext automatically."""
    def static_url(filename):
        """Generate full URL for static files."""
        return url_for('static', filename=filename)
    
    return {
        '_': gettext,
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

# Run the Flask app
if __name__ == '__main__':
    # This only runs with 'python app.py'
    if DEBUG:
        # Development: Use Flask's server
        app.run(host=HOST, port=PORT, debug=DEBUG)
    else:
        # Production: Warn the user
        print("For production, use: gunicorn -b 0.0.0.0:5000 app:app")
        app.run(host=HOST, port=PORT, debug=DEBUG)
