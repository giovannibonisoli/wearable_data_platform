"""
OAuth provider abstraction layer.

Switching between Fitbit and Google Health API is a single config change::

    OAUTH_PROVIDER=google   # in .env or deployment environment

All callers use the same functions — no import changes needed when switching.
"""

from typing import Callable, Optional

from config import OAUTH_PROVIDER


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def create_api_client(
    access_token: str,
    refresh_token: str,
    on_tokens_updated: Optional[Callable[[str, str], None]] = None,
):
    """Return an API client for the configured OAuth provider.

    The returned client has a ``get(url, optional=False)`` method that
    returns ``(data_dict_or_None, rate_limited_bool)``.
    """
    if OAUTH_PROVIDER == "google":
        from services.integrations.google_health import GoogleHealthClient
        return GoogleHealthClient(access_token, refresh_token, on_tokens_updated)

    from services.integrations.fitbit import FitbitClient
    return FitbitClient(access_token, refresh_token, on_tokens_updated)


# ---------------------------------------------------------------------------
# Auth URL generation
# ---------------------------------------------------------------------------

def generate_auth_url(state: str, code_challenge: str | None = None) -> str:
    """Build the OAuth authorization URL for the configured provider."""
    if OAUTH_PROVIDER == "google":
        from services.integrations.google_health import generate_google_auth_url
        return generate_google_auth_url(state)

    from services.integrations.fitbit import generate_auth_url as _fitbit_url
    if code_challenge is None:
        raise ValueError("code_challenge is required for Fitbit OAuth")
    return _fitbit_url(code_challenge, state)


# ---------------------------------------------------------------------------
# Token exchange
# ---------------------------------------------------------------------------

def exchange_code(code: str, code_verifier: str | None = None) -> tuple[str | None, str | None]:
    """Exchange an authorization code for access/refresh tokens."""
    if OAUTH_PROVIDER == "google":
        from services.integrations.google_health import exchange_google_code
        return exchange_google_code(code)

    from services.integrations.fitbit import get_tokens
    if code_verifier is None:
        raise ValueError("code_verifier is required for Fitbit OAuth")
    return get_tokens(code, code_verifier)
