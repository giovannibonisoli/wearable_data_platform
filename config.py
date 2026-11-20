"""
Configuration Module

Loads all configuration from the process environment (e.g. export on Linux,
systemd, Docker, or your hosting provider). Secrets must never be hardcoded.
"""

import os
from datetime import date
from urllib.parse import quote_plus


class ConfigurationError(Exception):
    """Raised when required configuration is missing or invalid"""
    pass


def get_required_env(var_name: str) -> str:
    """
    Get required environment variable or raise error.
    
    Args:
        var_name: Name of environment variable
        
    Returns:
        Value of environment variable
        
    Raises:
        ConfigurationError: If variable is not set or empty
    """
    value = os.getenv(var_name)
    if not value:
        raise ConfigurationError(
            f"Required environment variable '{var_name}' is not set. "
            f"Export it in the shell, or configure it in your process manager / container."
        )
    return value


def get_optional_env(var_name: str, default: str) -> str:
    """
    Get optional environment variable with default value.
    
    Args:
        var_name: Name of environment variable
        default: Default value if variable is not set
        
    Returns:
        Value of environment variable or default
    """
    return os.getenv(var_name, default)


def get_bool_env(var_name: str, default: bool = False) -> bool:
    """
    Get a boolean environment variable.

    Accepts the usual truthy spellings ("1", "true", "yes", "on"); anything
    else is False. Falls back to `default` when the variable is unset.
    """
    value = os.getenv(var_name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


# =============================================================================
# OAuth Configuration (Google Health API)
# =============================================================================
GOOGLE_CLIENT_ID = get_required_env("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = get_required_env("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = get_optional_env("GOOGLE_REDIRECT_URI", "http://localhost:5000/callback")

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
HEALTH_API_BASE = "https://health.googleapis.com/v4"


# =============================================================================
# Database Configuration
# =============================================================================
DB_CONFIG = {
    'host': get_required_env("DB_HOST"),
    'user': get_required_env("DB_USER"),
    'password': get_required_env("DB_PASSWORD"),
    'port': get_required_env("DB_PORT"),
    'database': get_required_env("DB_NAME"),
    'sslmode': get_optional_env("DB_SSLMODE", "require")
}


# =============================================================================
# Email Configuration
# =============================================================================
EMAIL_SENDER = get_required_env("EMAIL_SENDER")
EMAIL_PASSWORD = get_required_env("EMAIL_PASSWORD")
SMTP_SERVER = get_optional_env("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(get_optional_env("SMTP_PORT", "587"))


# =============================================================================
# Flask Configuration
# =============================================================================
SECRET_KEY = get_required_env("SECRET_KEY")
FLASK_ENV = get_optional_env("FLASK_ENV", "development")
IS_PRODUCTION = FLASK_ENV == "production"

# -----------------------------------------------------------------------------
# SQLAlchemy
# Use an explicit SQLALCHEMY_DATABASE_URI when provided, otherwise build it
# from the individual DB_* variables.
# -----------------------------------------------------------------------------
_explicit_uri = get_optional_env("SQLALCHEMY_DATABASE_URI", "")
if _explicit_uri:
    SQLALCHEMY_DATABASE_URI = _explicit_uri
    SQLALCHEMY_ENGINE_OPTIONS = {}
else:
    SQLALCHEMY_DATABASE_URI = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{quote_plus(DB_CONFIG['password'])}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    SQLALCHEMY_ENGINE_OPTIONS = {
        "connect_args": {"sslmode": DB_CONFIG["sslmode"]}
    }

SQLALCHEMY_TRACK_MODIFICATIONS = False

# -----------------------------------------------------------------------------
# Development server / runtime flags
# PRODUCTION_HOST and PRODUCTION_PORT are accepted as overrides of HOST and
# PORT for backwards compatibility with existing deployments.
# -----------------------------------------------------------------------------
HOST = get_optional_env(
    "HOST", "0.0.0.0" if IS_PRODUCTION else "localhost"
)
PORT = int(get_optional_env("PORT", "5000"))

if IS_PRODUCTION:
    HOST = get_optional_env("PRODUCTION_HOST", HOST)
    PORT = int(get_optional_env("PRODUCTION_PORT", str(PORT)))
    DEBUG = False
else:
    # Local development defaults to debug mode; set DEBUG=false to disable it.
    DEBUG = get_bool_env("DEBUG", default=True)

_ssl_cert = get_optional_env("SSL_CERT", "")
_ssl_key = get_optional_env("SSL_KEY", "")
SSL_CONTEXT = (_ssl_cert, _ssl_key) if _ssl_cert and _ssl_key else None

APP_PREFIX = get_optional_env("APP_PREFIX", "").rstrip("/")
if APP_PREFIX and not APP_PREFIX.startswith("/"):
    APP_PREFIX = f"/{APP_PREFIX}"

# =============================================================================
# Data Collection
# First date the collectors fetch for a device that has no checkpoint yet.
# Global for all devices; set it once before the first collection and do not
# change it afterwards (devices already collected ignore it anyway).
# Format: YYYY-MM-DD
# =============================================================================
_collection_start_date = get_optional_env("COLLECTION_START_DATE", "2025-01-21")
try:
    COLLECTION_START_DATE = date.fromisoformat(_collection_start_date)
except ValueError:
    raise ConfigurationError(
        f"COLLECTION_START_DATE must be in YYYY-MM-DD format, got '{_collection_start_date}'"
    )


# =============================================================================
# Validation (Optional - uncomment to validate on import)
# =============================================================================
def validate_config():
    """
    Validate configuration values.
    
    Raises:
        ConfigurationError: If configuration is invalid
    """
    # Validate SECRET_KEY length
    if len(SECRET_KEY) < 32:
        raise ConfigurationError(
            "SECRET_KEY must be at least 32 characters long for security. "
            "Generate one with: python -c 'import secrets; print(secrets.token_hex(32))'"
        )
    
    # Validate port is numeric
    try:
        int(DB_CONFIG['port'])
    except ValueError:
        raise ConfigurationError(f"DB_PORT must be numeric, got '{DB_CONFIG['port']}'")


# Uncomment to validate configuration when module is imported
# validate_config()


# =============================================================================
# Development Helper
# =============================================================================
def print_config(hide_secrets: bool = True):
    """
    Print current configuration (for debugging).
    
    Args:
        hide_secrets: If True, mask secret values
    """
    def mask(value: str) -> str:
        """Mask secret values"""
        if not hide_secrets or len(value) <= 4:
            return value
        return value[:2] + "*" * (len(value) - 4) + value[-2:]
    
    print("=" * 60)
    print("CONFIGURATION")
    print("=" * 60)
    print(f"Environment: {FLASK_ENV}")
    print(f"Debug: {DEBUG}")
    print(f"Host: {HOST}")
    print(f"Port: {PORT}")
    print()
    print("OAuth:")
    print(f"  GOOGLE_CLIENT_ID: {mask(GOOGLE_CLIENT_ID)}")
    print(f"  GOOGLE_CLIENT_SECRET: {mask(GOOGLE_CLIENT_SECRET)}")
    print(f"  GOOGLE_REDIRECT_URI: {GOOGLE_REDIRECT_URI}")
    print()
    print("Database:")
    print(f"  Host: {DB_CONFIG['host']}")
    print(f"  Port: {DB_CONFIG['port']}")
    print(f"  Database: {DB_CONFIG['database']}")
    print(f"  User: {DB_CONFIG['user']}")
    print(f"  Password: {mask(DB_CONFIG['password'])}")
    print()
    print("Email:")
    print(f"  Sender: {EMAIL_SENDER}")
    print(f"  Password: {mask(EMAIL_PASSWORD)}")
    print(f"  SMTP Server: {SMTP_SERVER}:{SMTP_PORT}")
    print("=" * 60)


# =============================================================================
# Usage Example
# =============================================================================
if __name__ == "__main__":
    """Test configuration when run directly"""
    try:
        validate_config()
        print("✅ Configuration is valid!")
        print()
        print_config(hide_secrets=True)
    except ConfigurationError as e:
        print(f"❌ Configuration Error: {e}")
        exit(1)
