"""
OAuth provider integration layer.

Currently backed by the Google Health API.
"""

import base64
import hashlib
import secrets
import string
from typing import Callable, Optional

from services.integrations.google_health import (
    GoogleHealthClient,
    RefreshTokenRevokedError,
    exchange_google_code,
    generate_google_auth_url,
)


def create_api_client(
    access_token: str,
    refresh_token: str,
    on_tokens_updated: Optional[Callable[[str, str], None]] = None,
    on_tokens_revoked: Optional[Callable[[], None]] = None,
):
    """Return a Google Health API client scoped to a single device.

    The returned client has a ``get(url, optional=False)`` method that
    returns ``(data_dict_or_None, rate_limited_bool)``.

    ``on_tokens_revoked`` is called once if the refresh token turns out to be
    dead; the call that triggered it then raises ``RefreshTokenRevokedError``.
    """
    return GoogleHealthClient(access_token, refresh_token, on_tokens_updated, on_tokens_revoked)


def generate_auth_url(state: str, code_challenge: str) -> str:
    """Build a Google OAuth 2.0 authorization URL (PKCE, S256)."""
    return generate_google_auth_url(state, code_challenge)


def exchange_code(code: str, code_verifier: str = "") -> tuple[str | None, str | None]:
    """Exchange an authorization code for Google access/refresh tokens."""
    return exchange_google_code(code, code_verifier)


def generate_state(length: int = 16) -> str:
    """Generate an unguessable state parameter for OAuth."""
    characters = string.ascii_letters + string.digits
    return "".join(secrets.choice(characters) for _ in range(length))


def generate_pkce_pair() -> tuple[str, str]:
    """Generate a PKCE (code_verifier, code_challenge) pair using S256.

    The verifier is 86 URL-safe characters (RFC 7636 requires 43-128).
    """
    code_verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    code_challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return code_verifier, code_challenge
