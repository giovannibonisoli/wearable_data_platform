"""
Result enums for service layer operations.
Used by routes to map outcomes to user-facing messages.
"""
from enum import Enum


class ChangePasswordResult(Enum):
    SUCCESS = "success"
    NO_CURRENT_PASSWORD = "no_current_password"
    ERROR = "error"


class AddDeviceResult(Enum):
    ADDED = "added"
    ALREADY_EXISTS = "already_exists"
    ERROR = "error"


class SendAuthEmailResult(Enum):
    SUCCESS = "success"
    EMAIL_SENDING_ERROR = "email_sending_error"
    ERROR_STORING_PENDING_AUTH = "error_storing_pending_auth"


class AuthGrantResult(Enum):
    SUCCESS = "success"
    MISSING_AUTH_INFO = "missing_auth_info"
    EMAIL_NOT_FOUND = "email_not_found"
    INVALID_AUTH_LINK = "invalid_auth_link"
    ERROR_RETRIEVE_TOKENS = "error_retrieve_tokens"
    ERROR_STATE_UPDATE = "error_state_update"


class CollectorResult(Enum):
    """Result of a Fitbit data collection run for one device."""
    SUCCESS = "success"
    UP_TO_DATE = "up_to_date"
    RATE_LIMITED = "rate_limited"
    ERROR = "error"

class AdminCreateCareProviderResult(Enum):
    SUCCESS = "success"
    ERROR = "error"


class AdminCreateStaffUserResult(Enum):
    SUCCESS = "success"
    USERNAME_EXISTS = "username_exists"
    ERROR = "error"


class AdminResetPasswordResult(Enum):
    SUCCESS = "success"
    NOT_FOUND = "not_found"
    FORBIDDEN = "forbidden"
    ERROR = "error"


class AdminDeactivateStaffUserResult(Enum):
    SUCCESS = "success"
    NOT_FOUND = "not_found"
    FORBIDDEN = "forbidden"
    ERROR = "error"
