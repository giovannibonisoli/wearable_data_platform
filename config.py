import os
from dotenv import load_dotenv

# Load enviroment variables from .env by forcing overwriting

load_dotenv(override=True)
ADMIN_MAIL = os.getenv("ADMIN_MAIL")
ADMIN_PSSW = os.getenv("ADMIN_PSSW")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

#  Environment-based redirect URL

# REDIRECT_URI = "http://localhost:5000/livelyageing/callback"
REDIRECT_URI = "https://tango.ing.unimo.it/livelyageing/callback"
# if os.getenv("FLASK_ENV") == "production":
#     REDIRECT_URI = "https://tango.ing.unimo.it/livelyageing/callback"
#     #REDIRECT_URI = "http://localhost:5000/livelyageing/callback"
# else:
#     # URL local para desarrollo
#     REDIRECT_URI = "http://localhost:5000/livelyageing/callback"
#     #REDIRECT_URI = "https://tango.ing.unimo.it/livelyageing/callback"

AUTH_URL = "https://www.fitbit.com/oauth2/authorize"
TOKEN_URL = "https://api.fitbit.com/oauth2/token"

DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'port': os.getenv("DB_PORT"),
    'database': os.getenv("DB_NAME"),
    "sslmode": "require"
}


# List of Fitbit Users (emails)
USERS = [
    # {"email": "Wearable1LivelyAgeign@gmail.com", "auth_code": None, "access_token": None, "refresh_token": None},
    # {"email": "Wearable2LivelyAgeign@gmail.com", "auth_code": None, "access_token": None, "refresh_token": None},
    {"email": "Wearable4LivelyAgeign@gmail.com ", "auth_code": None, "access_token": None, "refresh_token": None}
]

# Email configuration
EMAIL_SENDER = 'unimorefitbitapi@gmail.com'
EMAIL_PASSWORD = 'wbrx tzau yidm ctza'
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
