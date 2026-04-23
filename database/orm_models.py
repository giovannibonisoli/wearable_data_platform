"""
SQLAlchemy ORM models mapped to PostgreSQL tables.
"""

from extensions import db
from sqlalchemy import TIMESTAMP
from enum import Enum as PyEnum

class StatusType(PyEnum):
    """Stati per autorizzazione e device."""
    INSERTED = 'INSERTED'
    AUTHORIZED = 'AUTHORIZED'
    NON_ACTIVE = 'NON_ACTIVE'

    def __str__(self):
        return self.value

class UserRole(PyEnum):
    ADMIN = 'ADMIN'
    STAFF = 'STAFF'

    def __str__(self):
        return self.value


class UserModel(db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(255), unique=True, nullable=False)
    password_hash = db.Column(db.Text, nullable=False)
    full_name = db.Column(db.String(255), nullable=True)
    role = db.Column(
        db.Enum(UserRole, name='user_role'),
        nullable=False,
        server_default=UserRole.STAFF.value
    )

    created_at = db.Column(db.DateTime, nullable=True)
    last_login = db.Column(db.DateTime, nullable=True)
    is_active = db.Column(db.Boolean, nullable=False, default=True)


class StaffProfileModel(db.Model):
    __tablename__ = "staff_profiles"

    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), primary_key=True)
    care_provider_id = db.Column(db.Integer, db.ForeignKey("care_providers.id"), nullable=False)


class CareProviderModel(db.Model):
    __tablename__ = "care_providers"

    id = db.Column(db.Integer, primary_key=True)
    full_name = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, nullable=True)


class DeviceModel(db.Model):
    __tablename__ = "devices"

    id = db.Column(db.Integer, primary_key=True)
    care_provider_id = db.Column(db.Integer, db.ForeignKey("care_providers.id"), nullable=False)
    email_address = db.Column(db.String(255), nullable=False)
    authorization_status = db.Column(
        db.Enum(StatusType, name='status_type'),
        nullable=False,
        server_default=StatusType.INSERTED.value  # ← usa .value, non .name!
    )
    device_type = db.Column(db.String(128), nullable=True)
    created_at = db.Column(db.DateTime, nullable=True)
    last_synch = db.Column(db.DateTime, nullable=True)
    daily_summaries_checkpoint = db.Column(db.Date, nullable=True)
    intraday_checkpoint = db.Column(db.DateTime, nullable=True)
    sleep_checkpoint = db.Column(db.Date, nullable=True)
    access_token = db.Column(db.Text, nullable=True)
    refresh_token = db.Column(db.Text, nullable=True)


class PendingAuthorizationModel(db.Model):
    __tablename__ = "pending_authorizations"

    id = db.Column(db.Integer, primary_key=True)
    device_id = db.Column(db.Integer, db.ForeignKey("devices.id"), nullable=False)
    state = db.Column(db.Text, nullable=False)
    code_verifier = db.Column(db.Text, nullable=False)
    expires_at = db.Column(db.DateTime, nullable=False)
    created_at = db.Column(db.DateTime, nullable=True)


class DailySummaryModel(db.Model):
    __tablename__ = "daily_summaries"

    device_id = db.Column(db.Integer, db.ForeignKey("devices.id"), primary_key=True)
    date = db.Column(db.Date, primary_key=True)
    
    steps = db.Column(db.Integer, nullable=True)
    heart_rate = db.Column(db.Float, nullable=True)
    sleep_minutes = db.Column(db.Integer, nullable=True)
    calories = db.Column(db.Float, nullable=True)
    distance = db.Column(db.Float, nullable=True)
    floors = db.Column(db.Integer, nullable=True)
    elevation = db.Column(db.Float, nullable=True)
    active_minutes = db.Column(db.Integer, nullable=True)
    sedentary_minutes = db.Column(db.Integer, nullable=True)
    nutrition_calories = db.Column(db.Float, nullable=True)
    water = db.Column(db.Float, nullable=True)
    weight = db.Column(db.Float, nullable=True)
    bmi = db.Column(db.Float, nullable=True)
    fat = db.Column(db.Float, nullable=True)
    oxygen_saturation = db.Column(db.Float, nullable=True)
    respiratory_rate = db.Column(db.Float, nullable=True)
    temperature = db.Column(db.Float, nullable=True)



class IntradayMetricModel(db.Model):
    __tablename__ = "intraday_metrics"

    device_id = db.Column(db.Integer, db.ForeignKey("devices.id"), primary_key=True)
    time = db.Column(TIMESTAMP(timezone=True), primary_key=True) 
    heart_rate = db.Column(db.Float, nullable=True)
    steps = db.Column(db.Integer, nullable=True)
    calories = db.Column(db.Float, nullable=True)
    distance = db.Column(db.Float, nullable=True)


class SleepSessionModel(db.Model):
    __tablename__ = "sleep_sessions"

    id = db.Column(db.Integer, primary_key=True)
    device_id = db.Column(db.Integer, db.ForeignKey("devices.id"), nullable=False)


class SleepLogModel(db.Model):
    __tablename__ = "sleep_logs"

    sleep_session_id = db.Column(db.Integer, db.ForeignKey("sleep_sessions.id"), primary_key=True)
    start_time = db.Column(db.DateTime, primary_key=True)
    end_time = db.Column(db.DateTime, nullable=False)
    is_main_sleep = db.Column(db.Boolean, nullable=False)
    duration = db.Column(db.Integer, nullable=False)
    minutes_asleep = db.Column(db.Integer, nullable=False)
    minutes_awake = db.Column(db.Integer, nullable=False)
    minutes_in_the_bed = db.Column(db.Integer, nullable=False)
    log_type = db.Column(db.String(64), nullable=False)
    type = db.Column(db.String(64), nullable=False)


class SleepLevelModel(db.Model):
    __tablename__ = "sleep_levels"

    sleep_session_id = db.Column(db.Integer, db.ForeignKey("sleep_sessions.id"), primary_key=True)
    time = db.Column(db.DateTime, primary_key=True)
    level = db.Column(db.String(32), nullable=False)
    seconds = db.Column(db.Integer, nullable=False)


class SleepShortLevelModel(db.Model):
    __tablename__ = "sleep_short_levels"

    sleep_session_id = db.Column(db.Integer, db.ForeignKey("sleep_sessions.id"), primary_key=True)
    time = db.Column(db.DateTime, primary_key=True)
    seconds = db.Column(db.Integer, nullable=False)


class BreathingRateIntradayModel(db.Model):
    __tablename__ = "breathing_rate_intraday"
    
    sleep_session_id = db.Column(db.Integer, db.ForeignKey("sleep_sessions.id"), primary_key=True)
    time = db.Column(db.DateTime, primary_key=True)
    breathing_rate = db.Column(db.Float, nullable=False)


class SpO2IntradayModel(db.Model):
    __tablename__ = "spo2_intraday"
    
    sleep_session_id = db.Column(db.Integer, db.ForeignKey("sleep_sessions.id"), primary_key=True)
    time = db.Column(db.DateTime, primary_key=True)
    spo2_value = db.Column(db.Float, nullable=False)


class HRVIntradayModel(db.Model):
    __tablename__ = "hrv_intraday"
    
    sleep_session_id = db.Column(db.Integer, db.ForeignKey("sleep_sessions.id"), primary_key=True)
    time = db.Column(db.DateTime, primary_key=True)
    rmssd = db.Column(db.Float, nullable=False)
    lf = db.Column(db.Float, nullable=True)
    hf = db.Column(db.Float, nullable=True)
    coverage = db.Column(db.Float, nullable=True)
