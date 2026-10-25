# LivelyAging Wearable Data Platform

A Flask-based platform for collecting and managing wearable health data from wearable devices via the Google Health API.

## Features

- Google Health API integration for user authentication
- Staff user and care provider management
- Device pairing via QR codes
- Data collectors (intraday activities, daily summaries, sleep)
- Multi-language support (English, Italian, Spanish)
- Email notifications and password reset flows

## Requirements

- Python 3.9+
- PostgreSQL database
- Google Health API credentials (GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET)
- SMTP server for emails

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file with the following variables:

```bash
# Flask
SECRET_KEY=your-secret-key-min-32-chars
FLASK_ENV=development

# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=livelyageing
DB_USER=your-db-user
DB_PASSWORD=your-db-password

# Google Health API OAuth
GOOGLE_CLIENT_ID=your-google-client-id
GOOGLE_CLIENT_SECRET=your-google-client-secret
REDIRECT_URI=http://localhost:5000/

# Email (SMTP)
EMAIL_SENDER=your-email@example.com
EMAIL_PASSWORD=your-email-password
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587

# Optional
# DB_SSLMODE=require
```

## Running

```bash
# Development
python app.py

# Or with Flask CLI
flask run
```

The app runs at `http://localhost:5000/` by default.

## Database Migrations

```bash
flask db init
flask db migrate -m "Initial migration"
flask db upgrade
```

## Project Structure

```
├── app.py                 # Main Flask application
├── config.py              # Configuration module
├── extensions.py         # Flask extensions (db, migrate)
├── database/
│   ├── orm_models.py    # SQLAlchemy models
│   └── repositories/   # Data access layer
├── services/
│   ├── device_service.py
│   ├── staff_user_service.py
│   ├── care_provider_service.py
│   ├── collectors/     # Google Health data collectors
│   └── integrations/   # Google Health API, emails
├── templates/           # Jinja2 templates
├── static/             # CSS, JS, images
├── translations/       # i18n message files
└── tests/              # Unit tests
```

## Testing

```bash
coverage run -m pytest tests/
coverage report
```

## License

Proprietary - All rights reserved.