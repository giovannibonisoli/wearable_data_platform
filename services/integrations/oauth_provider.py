"""
OAuth provider integration layer.

Currently backed by the Google Health API.
"""

import random
import string
from typing import Callable, Optional

from services.integrations.google_health import (
    GoogleHealthClient,
    exchange_google_code,
    generate_google_auth_url,
)


def create_api_client(
    access_token: str,
    refresh_token: str,
    on_tokens_updated: Optional[Callable[[str, str], None]] = None,
):
    """Return a Google Health API client scoped to a single device.

    The returned client has a ``get(url, optional=False)`` method that
    returns ``(data_dict_or_None, rate_limited_bool)``.
    """
    return GoogleHealthClient(access_token, refresh_token, on_tokens_updated)


def generate_auth_url(state: str) -> str:
    """Build a Google OAuth 2.0 authorization URL."""
    return generate_google_auth_url(state)


def exchange_code(code: str) -> tuple[str | None, str | None]:
    """Exchange an authorization code for Google access/refresh tokens."""
    return exchange_google_code(code)


def generate_state(length: int = 16) -> str:
    """Generate a random state parameter for OAuth."""
    characters = string.ascii_letters + string.digits
    return "".join(random.choice(characters) for _ in range(length))
