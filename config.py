"""
Configuration Module

Loads all configuration from environment variables.
All secrets should be in .env file, NEVER hardcoded here!
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(override=True)


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
            f"Please add it to your .env file."
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


# =============================================================================
# OAuth Configuration (Fitbit)
# =============================================================================
CLIENT_ID = get_required_env("CLIENT_ID")
CLIENT_SECRET = get_required_env("CLIENT_SECRET")

# Environment-based redirect URL (different for dev/staging/prod)
REDIRECT_URI = get_optional_env("REDIRECT_URI", "http://localhost:5000/")

# Fitbit API URLs (constants - these don't change)
AUTH_URL = "https://www.fitbit.com/oauth2/authorize"
TOKEN_URL = "https://api.fitbit.com/oauth2/token"


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
DEBUG = FLASK_ENV == "development"

# Flask host and port
HOST = get_optional_env("HOST", "0.0.0.0" if FLASK_ENV == "production" else "localhost")
PORT = int(get_optional_env("PORT", "5000"))


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
    print(f"  CLIENT_ID: {mask(CLIENT_ID)}")
    print(f"  CLIENT_SECRET: {mask(CLIENT_SECRET)}")
    print(f"  REDIRECT_URI: {REDIRECT_URI}")
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
