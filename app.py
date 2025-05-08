from logging.handlers import RotatingFileHandler
from flask import Flask, logging, render_template, request, redirect, session, url_for, flash, g, jsonify, Response
from auth import generate_state, get_tokens, generate_code_verifier, generate_code_challenge, generate_auth_url
from db import DatabaseManager, get_daily_summaries, get_user_alerts, get_user_id_by_email
from config import CLIENT_ID, REDIRECT_URI
from translations import TRANSLATIONS
import os
from flask_login import current_user, login_user, logout_user, login_required
from flask_login import LoginManager, UserMixin
import logging
from datetime import datetime, timedelta, timezone
from flask_babel import Babel, get_locale, gettext as _

# Initialize Flask app
app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY')

# Configuración básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]',
    handlers=[
        logging.StreamHandler(),  # Log a consola
        logging.FileHandler('app.log', mode='w')  # Log a archivo, sobrescribiendo cada vez
    ]
)

# Obtener el modo de ejecución
FLASK_ENV = os.getenv('FLASK_ENV', 'development')  # Por defecto, modo desarrollo
USERNAME = os.getenv('log_USERNAME')  
PASSWORD = os.getenv('PASSWORD')

# Language settings
LANGUAGES = {
    'es': 'Español',
    'en': 'English'
}
DEFAULT_LANGUAGE = 'es'

# Initialize Babel
babel = Babel(app)

def select_locale():
    """Get the best language for the user."""
    # First try to get language from the session
    if 'language' in session:
        return session['language']
    return request.accept_languages.best_match(LANGUAGES.keys(), DEFAULT_LANGUAGE)

babel.init_app(app, locale_selector=select_locale)

@app.context_processor
def inject_globals():
    """Make common variables available to all templates."""
    return {
        'LANGUAGES': LANGUAGES,
        'get_locale': lambda: str(get_locale()),
        'current_language': lambda: session.get('language', DEFAULT_LANGUAGE),
        '_': _
    }

# Configurar Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'  # Ruta para el inicio de sesión


if FLASK_ENV == 'production':
    # Modo producción: usar IP pública y HTTPS
    HOST = os.getenv('PRODUCTION_HOST','0.0.0.0')
    PORT = int(os.getenv('PRODUCTION_PORT'))
    # SSL_CONTEXT = (
    #     os.getenv('SSL_CERT'),  # Ruta al certificado
    #     os.getenv('SSL_KEY')     # Ruta a la clave privada
    # )
    DEBUG = True
else:
    # Modo desarrollo: usar localhost y HTTP
    HOST = os.getenv('HOST')
    PORT = int(os.getenv('PORT'))
    SSL_CONTEXT = None
    DEBUG = os.getenv('DEBUG').lower() == 'true'
# Modelo de usuario
class User(UserMixin):
    def __init__(self, id):
        self.id = id

# Cargar el usuario
@login_manager.user_loader
def load_user(user_id):
    return User(user_id)

# Ruta de inicio de sesión
@app.route('/livelyageing/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('home'))  # Redirigir a home en lugar de index

    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        print(username)
        print(USERNAME)
        print(password)
        print(PASSWORD)
        if username == USERNAME and password == PASSWORD:
            user = User(username)
            login_user(user)
            return redirect(url_for('home'))  # Redirigir a home en lugar de index
        else:
            flash('Usuario o contraseña incorrectos', 'danger')
    return render_template('login.html')

# Ruta de cierre de sesión
@app.route('/livelyageing/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

# Proteger todas las rutas con @login_required
@app.before_request
def require_login():
    if not current_user.is_authenticated and request.endpoint != 'login':
        return redirect(url_for('login'))

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
    Render the dashboard homepage.
    This will display the Fitbit data stored in the database.
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
            
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return render_template('dashboard_content.html', 
                                       daily_summaries=[],
                                    intraday_metrics=intraday_metrics_4col,
                                    sleep_logs=sleep_logs)
            else:
                return render_template('alerts_dashboard.html', 
                                       daily_summaries=[],
                                    intraday_metrics=intraday_metrics_4col,
                                    sleep_logs=sleep_logs)
        except Exception as e:
            app.logger.error(f"Error fetching data for dashboard: {e}")
            error = "No se pudieron obtener los datos para el dashboard."
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return render_template('dashboard_content.html', 
                                    daily_summaries=[],
                                    intraday_metrics=[],
                                    sleep_logs=[],
                                    error=error)
            else:
                return render_template('alerts_dashboard.html', 
                                    daily_summaries=[],
                                    intraday_metrics=[],
                                    sleep_logs=[],
                                    error=error)
        finally:
            db.close()
    else:
        error = "No se pudo conectar a la base de datos."
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return render_template('dashboard_content.html', 
                                daily_summaries=[],
                                intraday_metrics=[],
                                sleep_logs=[],
                                error=error)
        else:
            return render_template('alerts_dashboard.html', 
                                daily_summaries=[],
                                intraday_metrics=[],
                                sleep_logs=[],
                                error=error)

@app.route('/livelyageing/home')
@login_required
def home():
    """
    Render the home page with recent activity.
    """
    db = DatabaseManager()
    if db.connect():
        try:
            # Get recent users with their latest activity
            recent_users = db.execute_query("""
                SELECT u.id, u.name, u.email, 
                       MAX(d.date) as created_at
                FROM users u
                LEFT JOIN daily_summaries d ON u.id = d.user_id
                GROUP BY u.id, u.name, u.email
                ORDER BY created_at DESC NULLS LAST
                LIMIT 10
            """)
            
            # Convertir la fecha a datetime para evitar el error de tipo
            now = datetime.now()
            processed_users = []
            for user in recent_users:
                user_list = list(user)  # Convertir tupla a lista para poder modificar
                if user_list[3]:  # Si created_at no es None
                    # Convertir date a datetime usando datetime.combine
                    user_list[3] = datetime.combine(user_list[3], datetime.min.time())
                processed_users.append(tuple(user_list))  # Volver a convertir a tupla
            
            return render_template('home.html', recent_users=processed_users, now=now)
        except Exception as e:
            app.logger.error(f"Error fetching data for home page: {e}")
            return "Error: No se pudieron obtener los datos para la página de inicio.", 500
        finally:
            db.close()
    else:
        return "Error: No se pudo conectar a la base de datos.", 500

@app.route('/livelyageing/user_stats')
@login_required
def user_stats():
    """
    Display statistics for all users.
    """
    db = DatabaseManager()
    if db.connect():
        try:
            # Get all users
            users = db.execute_query("""
                SELECT id, name, email, created_at
                FROM users
                ORDER BY name
            """)
            
            return render_template('user_stats.html', users=users)
        except Exception as e:
            app.logger.error(f"Error fetching user statistics: {e}")
            return "Error: No se pudieron obtener las estadísticas de usuarios.", 500
        finally:
            db.close()
    else:
        return "Error: No se pudo conectar a la base de datos.", 500

# Route: Link a new Fitbit device
@app.route('/livelyageing/link', methods=['GET', 'POST'])
@login_required
def link_device():
    """
    Handle the linking of a new Fitbit device.
    """
    if request.method == 'POST':
        email = request.form.get('email')
        if not email:
            flash('Por favor, proporciona un email.', 'danger')
            return redirect(url_for('link_device'))
            
        # Store email in session and redirect to assign_user
        session['pending_email'] = email
        return redirect(url_for('assign_user'))
    
    # GET request - show form with available emails
    db = DatabaseManager()
    if not db.connect():
        flash('Error de conexión a la base de datos.', 'danger')
        return redirect(url_for('index'))
        
    try:
        emails = db.execute_query("SELECT DISTINCT email FROM users")
        return render_template('link_device.html', emails=emails)
    except Exception as e:
        flash(f'Error: {str(e)}', 'danger')
        return redirect(url_for('index'))
    finally:
        db.close()

@app.route('/livelyageing/assign', methods=['GET', 'POST'])
@login_required
def assign_user():
    """
    Handle the assignment of a new user.
    """
    if request.method == 'POST':
        user_name = request.form['user_name']
        
        # Check if the user name already exists in the database (case-insensitive)
        db = DatabaseManager()
        if db.connect():
            try:
                # Query to check if the user name already exists (case-insensitive)
                existing_user = db.execute_query("SELECT name FROM users WHERE LOWER(name) = LOWER(%s)", (user_name,))
                
                if existing_user:
                    # If the user name already exists, show an error message
                    error = f"El nombre de usuario '{user_name}' ya está en uso."
                    return render_template('assign_user.html', error=error)
                else:
                    # If the user name is not in use, proceed to authorization
                    # Store the new name in the session for later use
                    session['new_user_name'] = user_name
                    
                    # Make sure we have a pending_email in the session
                    if 'pending_email' not in session:
                        # If no pending_email, redirect back to link_device
                        return redirect(url_for('link_device'))
                    
                    code_verifier = generate_code_verifier()
                    code_challenge = generate_code_challenge(code_verifier)
                    state = generate_state()
                    print(f"Generated valid state: {state}")
                    print(f"Generated code verifier: {code_verifier}")
                    print(f"Generated code challenge: {code_challenge}")
                    auth_url = generate_auth_url(code_challenge, state)
                    print(f"Generated auth URL: {auth_url}")
                    session['code_verifier'] = code_verifier
                    session['state'] = state
                    return render_template('link_auth.html', auth_url=auth_url)
            except Exception as e:
                print(f"Error checking user name in database: {e}")
                return "Error: No se pudo verificar el nombre de usuario.", 500
            finally:
                db.close()
        else:
            return "Error: No se pudo conectar a la base de datos.", 500
    
    else:
        # If it's a GET request, check if we have a pending_email in the session
        if 'pending_email' not in session:
            # If no pending_email, redirect back to link_device
            return redirect(url_for('link_device'))
        
        # Render the assign_user.html template
        return render_template('assign_user.html')

# Route: Fitbit OAuth callback
@app.route('/livelyageing/callback')
@login_required
def callback():
    """
    Handle the callback from Fitbit after the user authorizes the app.
    This route captures the authorization code and exchanges it for access and refresh tokens.
    """
    try:
        code = request.args.get('code')
        returned_state = request.args.get('state')
        stored_state = session.get('state')
        
        if returned_state != stored_state:
            app.logger.error("Invalid state parameter. Possible CSRF attack.")
            flash("Error: Invalid state parameter. Possible CSRF attack.", "danger")
            return redirect(url_for('link_device'))
        
        email = session.get('pending_email')
        new_user_name = session.get('new_user_name')
        code_verifier = session.get('code_verifier')
        
        if not all([email, new_user_name, code_verifier, code]):
            app.logger.error("Missing required session variables or authorization code")
            flash("Error: Missing required information. Please try again.", "danger")
            return redirect(url_for('link_device'))

        db = DatabaseManager()
        if db.connect():
            try:
                # Query to check if the email is already in use
                existing_user = db.get_user_by_email(email)

                if existing_user:
                    user_id, existing_access_token, existing_refresh_token = existing_user

                    # Flow 2: Reassign the device to a new user
                    if new_user_name:
                        if not existing_access_token or not existing_refresh_token:
                            if code:
                                access_token, refresh_token = get_tokens(code, code_verifier)
                                db.add_user(new_user_name, email, access_token, refresh_token)
                                app.logger.info(f"Dispositivo reasignado a {new_user_name} ({email}) con nuevos tokens.")
                            else:
                                app.logger.error("Se requiere autorización para reasignar el dispositivo.")
                                flash("Error: Se requiere autorización para reasignar el dispositivo.", "danger")
                                return redirect(url_for('link_device'))
                        else:
                            db.add_user(new_user_name, email, existing_access_token, existing_refresh_token)
                            app.logger.info(f"Dispositivo reasignado a {new_user_name} ({email}) sin necesidad de reautorización.")
                    else:
                        app.logger.error("Se requiere un nombre de usuario para reasignar el dispositivo.")
                        flash("Error: Se requiere un nombre de usuario para reasignar el dispositivo.", "danger")
                        return redirect(url_for('assign_user'))
                else:
                    # Flow 1: Link a new email to a user
                    if new_user_name:
                        if code:
                            access_token, refresh_token = get_tokens(code, code_verifier)
                            db.add_user(new_user_name, email, access_token, refresh_token)
                            app.logger.info(f"Nuevo usuario {new_user_name} ({email}) añadido.")
                        else:
                            app.logger.error("Se requiere autorización para vincular un nuevo correo.")
                            flash("Error: Se requiere autorización para vincular un nuevo correo.", "danger")
                            return redirect(url_for('link_device'))
                    else:
                        app.logger.error("Se requiere un nombre de usuario para vincular un nuevo correo.")
                        flash("Error: Se requiere un nombre de usuario para vincular un nuevo correo.", "danger")
                        return redirect(url_for('assign_user'))

                # Clear the session data
                session.pop('pending_email', None)
                session.pop('new_user_name', None)
                session.pop('code_verifier', None)
                session.pop('state', None)

                return render_template('confirmation.html', user_name=new_user_name, email=email)
            except Exception as e:
                app.logger.error(f"Error during token exchange: {e}")
                flash(f"Error durante el intercambio de tokens: {e}", "danger")
                return redirect(url_for('link_device'))
            finally:
                db.close()
        else:
            app.logger.error("No se pudo conectar a la base de datos.")
            flash("Error: No se pudo conectar a la base de datos.", "danger")
            return redirect(url_for('link_device'))
    except Exception as e:
        app.logger.error(f"Unexpected error: {e}")
        flash(f"Error inesperado: {e}", "danger")
        return redirect(url_for('link_device'))

@app.route('/livelyageing/reassign', methods=['POST'])
@login_required
def reassign_device():
    """
    Handle the reassignment of a Fitbit device to a new user.
    """
    email = request.form['email']
    new_user_name = request.form['new_user_name']
    
    # Store the email and new user name in the session for later use
    session['pending_email'] = email
    session['new_user_name'] = new_user_name
    
    # Check if reauthorization is needed
    db = DatabaseManager()
    if db.connect():
        try:
            # Query to check if the email is already in use and has valid tokens
            existing_user = db.execute_query("SELECT access_token, refresh_token FROM users WHERE email = %s ORDER BY created_at DESC LIMIT 1", (email,))
            
            if existing_user:
                existing_access_token, existing_refresh_token = existing_user
                if not existing_access_token or not existing_refresh_token:
                    # If tokens are missing, require reauthorization
                    code_verifier = generate_code_verifier()
                    code_challenge = generate_code_challenge(code_verifier)
                    state = generate_state()
                    print(f"Generated valid state: {state}")
                    print(f"Generated code verifier: {code_verifier}")
                    print(f"Generated code challenge: {code_challenge}")
                    auth_url = generate_auth_url(code_challenge, state)
                    print(f"Generated auth URL: {auth_url}")
                    session['code_verifier'] = code_verifier
                    session['state'] = state
                    return render_template('link_auth.html', auth_url=auth_url)
                else:
                    # If tokens are valid, proceed to add the new user without reauthorization
                    db.add_user(new_user_name, email, existing_access_token, existing_refresh_token)
                    print(f"Dispositivo reasignado a {new_user_name} ({email}) sin necesidad de reautorización.")
                    return redirect('/')
            else:
                return "Error: El correo no está en uso.", 400
        except Exception as e:
            return f"Error: {e}", 400
        finally:
            db.close()
    else:
        return "Error: No se pudo conectar a la base de datos.", 500

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
    """Make translation function available in templates."""
    return {
        'get_text': get_text,
        'current_language': get_locale
    }

@app.route('/livelyageing/change_language')
def change_language():
    """Change the application language."""
    lang = request.args.get('lang', DEFAULT_LANGUAGE)
    if lang in LANGUAGES:
        session['language'] = lang
    return redirect(request.referrer or url_for('home'))

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
        from fitbit_intraday import process_emails as process_intraday_emails
        
        # Process daily data
        process_emails(emails)
        # Process intraday data
        process_intraday_emails(emails)
        
        return jsonify({'success': True})
    except Exception as e:
        app.logger.error(f"Error refreshing data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/livelyageing/api/daily_summary')
@login_required
def get_daily_summary():
    """
    Obtiene el resumen diario más reciente del usuario actual.
    """
    try:
        user_id = get_user_id_by_email(current_user.email)
        if not user_id:
            return jsonify({'error': 'Usuario no encontrado'}), 404

        # Obtener el resumen más reciente
        summaries = get_daily_summaries(
            user_id=user_id,
            start_date=datetime.now() - timedelta(days=1),
            end_date=datetime.now()
        )

        if not summaries:
            return jsonify({'error': 'No hay datos disponibles'}), 404

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
        app.logger.error(f"Error al obtener el resumen diario: {str(e)}")
        return jsonify({'error': 'Error interno del servidor'}), 500

@app.route('/livelyageing/api/alerts')
@login_required
def get_user_alerts_api():
    """
    Obtiene las alertas más recientes del usuario actual.
    """
    try:
        user_id = get_user_id_by_email(current_user.email)
        if not user_id:
            return jsonify({'error': 'Usuario no encontrado'}), 404

        # Obtener alertas de las últimas 24 horas
        alerts = get_user_alerts(
            user_id=user_id,
            start_time=datetime.now() - timedelta(hours=24),
            end_time=datetime.now(),
            acknowledged=False
        )

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
        db = DatabaseManager()
        if not db.connect():
            app.logger.error("No se pudo conectar a la base de datos")
            return jsonify({'error': 'Database connection error'}), 500

        # Obtener parámetros de filtrado
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        priority = request.args.get('priority')
        acknowledged = request.args.get('acknowledged')
        user_query = request.args.get('user_query')
        alert_type = request.args.get('alert_type')
        urgent_only = request.args.get('urgent_only') == 'on'
        page = request.args.get('page', 1, type=int)
        per_page = 10  # Número de alertas por página

        app.logger.info(f"Parámetros de filtrado: date_from={date_from}, date_to={date_to}, priority={priority}, acknowledged={acknowledged}, user_query={user_query}, alert_type={alert_type}, urgent_only={urgent_only}")

        # Crear diccionario de filtros para la paginación
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

        # Construir la consulta base
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

        # Aplicar filtros
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

        # Ordenar por prioridad y fecha descendente
        query += " ORDER BY CASE a.priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 END, a.alert_time DESC"

        app.logger.info(f"Query: {query}")
        app.logger.info(f"Params: {params}")

        try:
            # Obtener el total de alertas para la paginación
            count_query = f"SELECT COUNT(*) FROM ({query}) AS count_query"
            total = db.execute_query(count_query, params)[0][0]
            app.logger.info(f"Total de alertas encontradas: {total}")

            # Aplicar paginación
            query += " LIMIT %s OFFSET %s"
            params.extend([per_page, (page - 1) * per_page])

            # Ejecutar la consulta
            alerts_data = db.execute_query(query, params)
            app.logger.info(f"Alertas obtenidas: {len(alerts_data) if alerts_data else 0}")

            if not alerts_data:
                app.logger.warning("No se encontraron alertas con los filtros actuales")
                return render_template('alerts_dashboard.html', 
                                    alerts=[], 
                                    pagination=None,
                                    filters_dict=filters_dict,
                                    now=datetime.now(timezone.utc))

            # Convertir las tuplas en diccionarios con nombres de atributos
            alerts = []
            for alert in alerts_data:
                try:
                    # Obtener datos intradía para la alerta
                    intraday_data = {}
                    alert_type = alert[3]
                    base_alert_type = alert_type.split('_')[0] if '_' in alert_type else alert_type
                    
                    # Solo obtener datos intradía para tipos compatibles
                    if base_alert_type in ['heart_rate', 'steps', 'calories', 'active_zone_minutes']:
                        start_time = alert[1] - timedelta(hours=24)
                        end_time = alert[1]
                        intraday_metrics = db.execute_query("""
                            SELECT time, value 
                            FROM intraday_metrics 
                            WHERE user_id = %s 
                            AND type = %s 
                            AND time BETWEEN %s AND %s 
                            ORDER BY time
                        """, (alert[2], base_alert_type, start_time, end_time))
                        
                        if intraday_metrics:
                            intraday_data = {
                                'times': [m[0].strftime('%H:%M') for m in intraday_metrics],
                                'values': [float(m[1]) for m in intraday_metrics]
                            }
                            app.logger.info(f"Datos intradía obtenidos para {base_alert_type}: {len(intraday_metrics)} registros")
                        else:
                            app.logger.info(f"No se encontraron datos intradía para {base_alert_type}")

                    # Convertir el datetime a string formateado
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

            app.logger.info(f"Alertas procesadas: {len(alerts)}")

            # Crear objeto de paginación
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

            # Asegurarse de que now sea timezone-aware
            now = datetime.now(timezone.utc)

            return render_template('alerts_dashboard.html', 
                                alerts=alerts, 
                                pagination=pagination, 
                                filters_dict=filters_dict,
                                now=now)

        except Exception as e:
            app.logger.error(f"Error en la consulta SQL: {e}")
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

        # Obtener detalles de la alerta
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
            return jsonify({'error': 'Alerta no encontrada'}), 404

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
        app.logger.error(f"Error al obtener detalles de la alerta: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/livelyageing/api/alerts/<int:alert_id>/acknowledge', methods=['POST'])
@login_required
def acknowledge_alert(alert_id):
    try:
        db = DatabaseManager()
        if not db.connect():
            return jsonify({'success': False, 'error': 'Error de conexión a la base de datos'}), 500

        try:
            # Verificar si la alerta existe y no está reconocida
            check_query = "SELECT acknowledged FROM alerts WHERE id = %s"
            result = db.execute_query(check_query, [alert_id])
            
            if not result:
                return jsonify({'success': False, 'error': 'Alerta no encontrada'}), 404
                
            if result[0][0]:
                return jsonify({'success': False, 'error': 'La alerta ya está reconocida'}), 400
                
            # Actualizar solo el campo acknowledged
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
    Renderiza la ficha de usuario con la información básica y datos recientes.
    El resto de datos se cargan vía AJAX.
    """
    db = DatabaseManager()
    if not db.connect():
        return "Error: No se pudo conectar a la base de datos.", 500
    try:
        # Obtener datos básicos del usuario
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
            return "Usuario no encontrado", 404
            
        # Convertir la tupla en un diccionario
        user = {
            'id': user_data[0][0],
            'name': user_data[0][1],
            'email': user_data[0][2],
            'created_at': user_data[0][3],
            'access_token': user_data[0][4],
            'refresh_token': user_data[0][5],
            'age': int(user_data[0][6]) if user_data[0][6] else None
        }
        
        # Obtener el último resumen diario para datos actuales
        latest_summary = db.execute_query(
            """
            SELECT * FROM daily_summaries 
            WHERE user_id = %s 
            ORDER BY date DESC 
            LIMIT 1
            """, (user_id,)
        )
        
        # Convertir el resumen diario en un diccionario si existe
        if latest_summary:
            columns = [desc[0] for desc in db.cursor.description]
            latest_summary = dict(zip(columns, latest_summary[0]))
        
        # Obtener alertas recientes no reconocidas
        recent_alerts = db.execute_query(
            """
            SELECT * FROM alerts 
            WHERE user_id = %s 
            AND alert_time >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY alert_time DESC
            """, (user_id,)
        )
        
        # Convertir las alertas en diccionarios
        if recent_alerts:
            alert_columns = [desc[0] for desc in db.cursor.description]
            recent_alerts = [dict(zip(alert_columns, alert)) for alert in recent_alerts]
        
        return render_template('user_detail.html', 
                             user=user,
                             latest_summary=latest_summary,
                             recent_alerts=recent_alerts)
    except Exception as e:
        app.logger.error(f"Error al cargar la ficha de usuario: {e}")
        return "Error interno del servidor", 500
    finally:
        db.close()

@app.route('/livelyageing/api/user/<int:user_id>/daily_summary')
@login_required
def api_user_daily_summary(user_id):
    """
    Devuelve el resumen diario para un usuario y una fecha (por defecto hoy).
    """
    date_str = request.args.get('date')
    if date_str:
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            return jsonify({'error': 'Formato de fecha inválido'}), 400
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
    Devuelve los datos intradía para un usuario, fecha y tipo de métrica.
    """
    date_str = request.args.get('date')
    metric_type = request.args.get('type')
    
    if not metric_type:
        return jsonify({'error': 'Falta el tipo de métrica'}), 400
        
    if date_str:
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            return jsonify({'error': 'Formato de fecha inválido'}), 400
    else:
        date = datetime.now().date()
        
    db = DatabaseManager()
    if not db.connect():
        return jsonify({'error': 'DB error'}), 500
    try:
        start_time = datetime.combine(date, datetime.min.time())
        end_time = datetime.combine(date, datetime.max.time())
        
        data = db.execute_query(
            """
            SELECT time, value 
            FROM intraday_metrics 
            WHERE user_id = %s 
            AND type = %s 
            AND time BETWEEN %s AND %s 
            ORDER BY time
            """, (user_id, metric_type, start_time, end_time)
        )
        
        return jsonify({
            'intraday': [
                {
                    'time': row[0].strftime('%H:%M'),
                    'value': float(row[1])
                } for row in data
            ]
        })
    finally:
        db.close()

@app.route('/livelyageing/api/user/<int:user_id>/weekly_summary')
@login_required
def api_user_weekly_summary(user_id):
    """
    Devuelve los resúmenes diarios de los últimos 7 días para el usuario.
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
    Devuelve las alertas de los últimos 7 días para el usuario.
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
        return "Error de conexión a la base de datos", 500
    try:
        # Obtener filtros igual que en alerts_dashboard
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        priority = request.args.get('priority')
        acknowledged = request.args.get('acknowledged')
        user_query = request.args.get('user_query')
        # Construir la consulta base
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
        # Crear CSV con BOM UTF-8 para compatibilidad con Excel
        si = StringIO()
        cw = csv.writer(si)
        cw.writerow(["Fecha/Hora", "Usuario", "Email", "Tipo de Alerta", "Prioridad", "Valor Disparador", "Umbral", "Detalles", "Reconocida"])
        for a in alerts:
            cw.writerow([
                a[0].strftime('%Y-%m-%d %H:%M'),
                a[1], a[2], a[3], a[4], a[5], a[6], a[7], "Sí" if a[8] else "No"
            ])
        output = '\ufeff' + si.getvalue()  # Añadir BOM UTF-8
        si.close()
        fecha = datetime.now().strftime('%Y%m%d')
        return Response(
            output,
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment;filename=alertas_{fecha}.csv"}
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
        return "Error de conexión a la base de datos", 500
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
        cw.writerow(["Fecha/Hora", "Usuario", "Email", "Tipo de Alerta", "Prioridad", "Valor Disparador", "Umbral", "Detalles", "Reconocida"])
        for a in alerts:
            cw.writerow([
                a[0].strftime('%Y-%m-%d %H:%M'),
                a[1], a[2], a[3], a[4], a[5], a[6], a[7], "Sí" if a[8] else "No"
            ])
        output = '\ufeff' + si.getvalue()
        si.close()
        fecha = datetime.now().strftime('%Y%m%d')
        return Response(
            output,
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment;filename=alertas_usuario_{user_id}_{fecha}.csv"}
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
        return "Error de conexión a la base de datos", 500
    try:
        # Obtener fechas y métricas seleccionadas
        dates = request.args.getlist('dates')
        metrics = request.args.getlist('metrics')
        if not dates or not metrics:
            return "Debe seleccionar al menos una fecha y una métrica", 400
        # Preparar consulta
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
        cw.writerow(["Fecha", "Hora", "Métrica", "Valor"])
        for r in rows:
            cw.writerow(r)
        output = '\ufeff' + si.getvalue()
        si.close()
        fecha = datetime.now().strftime('%Y%m%d')
        return Response(
            output,
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment;filename=intradia_usuario_{user_id}_{fecha}.csv"}
        )
    finally:
        db.close()

# Run the Flask app
if __name__ == '__main__':
    # app.run(host=HOST, port=PORT, debug=DEBUG)
    app.run(debug=True)
