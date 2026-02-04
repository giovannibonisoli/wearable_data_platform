import os
from dotenv import load_dotenv

# Load enviroment variables from .env by forcing overwriting

load_dotenv(override=True)
ADMIN_MAIL = os.getenv("ADMIN_MAIL")
ADMIN_PSSW = os.getenv("ADMIN_PSSW")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

#  Environment-based redirect URL
REDIRECT_URI = "http://localhost:5000"

AUTH_URL = "https://www.fitbit.com/oauth2/authorize"
TOKEN_URL = "https://api.fitbit.com/oauth2/token"

DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'port': os.getenv("DB_PORT"),
    'database': os.getenv("DB_NAME"),
    'sslmode': "require"
}

# Email configuration
EMAIL_SENDER = "chingchungchang@gmail.com"
EMAIL_PASSWORD = "thybghhghhhhf"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
