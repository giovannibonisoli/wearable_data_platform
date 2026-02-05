from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional


@dataclass
class AdminUser:
    """Represents an admin user in the system."""
    id: int
    username: str
    full_name: str
    email: Optional[str] = None
    created_at: Optional[datetime] = None
    last_login: Optional[datetime] = None
    is_active: bool = True


@dataclass
class Device:
    """Represents a device/email address connected to the system."""
    id: int
    email_address: str
    authorization_status: str
    admin_user_id: int
    device_type: Optional[str] = None
    created_at: Optional[datetime] = None
    last_synch: Optional[datetime] = None
    daily_summaries_checkpoint: Optional[date] = None
    intraday_checkpoint: Optional[datetime] = None
    sleep_checkpoint: Optional[date] = None


@dataclass
class DailySummary:
    """Represents aggregated daily health metrics."""
    id: int
    device_id: int
    date: date
    steps: Optional[int] = None
    heart_rate: Optional[float] = None
    sleep_minutes: Optional[int] = None
    calories: Optional[float] = None
    distance: Optional[float] = None
    floors: Optional[int] = None
    elevation: Optional[float] = None
    active_minutes: Optional[int] = None
    sedentary_minutes: Optional[int] = None
    nutrition_calories: Optional[float] = None
    water: Optional[float] = None
    weight: Optional[float] = None
    bmi: Optional[float] = None
    fat: Optional[float] = None
    oxygen_saturation: Optional[float] = None
    respiratory_rate: Optional[float] = None
    temperature: Optional[float] = None


@dataclass
class IntradayMetric:
    """Represents a time-series health metric data point."""
    id: int
    device_id: int
    time: datetime
    heart_rate: Optional[float] = None
    steps: Optional[int] = None
    calories: Optional[float] = None
    distance: Optional[float] = None


@dataclass
class SleepSession:
    """Represents a sleep session grouping."""
    id: int
    device_id: int
    created_at: Optional[datetime] = None


@dataclass
class SleepLog:
    """Represents a detailed sleep segment."""
    id: int
    sleep_session_id: int
    start_time: datetime
    end_time: datetime
    is_main_sleep: bool
    duration: int
    minutes_asleep: int
    minutes_awake: int
    minutes_in_the_bed: int
    log_type: str
    type: str


@dataclass
class SleepLevel:
    """Represents a sleep level entry (REM, deep, light, awake)."""
    id: int
    sleep_session_id: int
    time: datetime
    level: str
    seconds: int


@dataclass
class Alert:
    """Represents a health alert/notification."""
    id: int
    email_id: int
    alert_type: str
    priority: str
    triggering_value: float
    threshold_value: str
    alert_time: datetime
    details: Optional[str] = None
    acknowledged: bool = False


@dataclass
class PendingAuthorization:
    """Represents a pending OAuth authorization."""
    id: int
    device_id: int
    state: str
    code_verifier: str
    expires_at: datetime
    created_at: Optional[datetime] = None
