import base64
import hashlib
import logging
import os
import random
import string
import requests

from datetime import datetime
from typing import Callable, Optional
from config import AUTH_URL, CLIENT_ID, CLIENT_SECRET, REDIRECT_URI

logger = logging.getLogger(__name__)

TOKEN_URL = "https://api.fitbit.com/oauth2/token"


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def get_tokens(code: str, code_verifier: str) -> tuple[str | None, str | None]:
    """Exchange an auth code for access/refresh tokens using PKCE."""
    auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    payload = {
        "client_id": CLIENT_ID,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "code_verifier": code_verifier,
    }

    logger.debug(f"Requesting tokens with payload: {payload}")
    response = requests.post(TOKEN_URL, data=payload, headers=headers)
    logger.debug(f"Token response status: {response.status_code}")

    if response.status_code != 200:
        raise Exception(f"Fitbit error: {response.text}")

    tokens = response.json()
    return tokens.get("access_token"), tokens.get("refresh_token")


def refresh_tokens(refresh_token: str) -> tuple[str | None, str | None]:
    """Obtain a new access/refresh token pair from an existing refresh token."""
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    response = requests.post(TOKEN_URL, data=payload)
    tokens = response.json()
    return tokens.get("access_token"), tokens.get("refresh_token")


def generate_state(length: int = 16) -> str:
    """Generate a random state parameter for OAuth."""
    characters = string.ascii_letters + string.digits
    return "".join(random.choice(characters) for _ in range(length))


def generate_code_verifier() -> str:
    """Generate a random PKCE code verifier."""
    return base64.urlsafe_b64encode(os.urandom(32)).rstrip(b"=").decode("utf-8")


def generate_code_challenge(code_verifier: str) -> str:
    """Derive the PKCE code challenge (S256) from a code verifier."""
    if isinstance(code_verifier, bytes):
        code_verifier = code_verifier.decode("utf-8")
    sha256 = hashlib.sha256(code_verifier.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(sha256).rstrip(b"=").decode("utf-8")


def generate_auth_url(code_challenge: str, state: str) -> str:
    """Build the Fitbit OAuth authorization URL."""
    from urllib.parse import urlencode

    scopes = (
        "activity cardio_fitness electrocardiogram heartrate "
        "irregular_rhythm_notifications location nutrition oxygen_saturation "
        "profile respiratory_rate settings sleep social temperature weight"
    )
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "scope": scopes,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "state": state,
        "redirect_uri": REDIRECT_URI,
    }
    auth_url = f"{AUTH_URL}?{urlencode(params)}"
    logger.debug(f"Generated auth URL: {auth_url}")
    return auth_url


# ---------------------------------------------------------------------------
# FitbitClient
# ---------------------------------------------------------------------------

class FitbitClient:
    """
    Stateful Fitbit API client scoped to a single device.

    Handles token refresh transparently: when a request returns 401 the client
    refreshes the token pair once, persists the new tokens via the
    ``on_tokens_updated`` callback, and retries the original request.

    Usage
    -----
    client = FitbitClient(
        access_token=access_token,
        refresh_token=refresh_token,
        on_tokens_updated=lambda a, r: device_repo.update_tokens(device_id, a, r),
    )

    data, rate_limited = client.get(url)
    device_info = client.get_device_info()
    """

    def __init__(
        self,
        access_token: str,
        refresh_token: str,
        on_tokens_updated: Optional[Callable[[str, str], None]] = None,
    ):
        """
        Args:
            access_token:       Current OAuth access token for this device.
            refresh_token:      Current OAuth refresh token for this device.
            on_tokens_updated:  Optional callback(new_access, new_refresh) invoked
                                immediately after a successful token refresh so that
                                the caller can persist the new tokens to the database.
        """
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.on_tokens_updated = on_tokens_updated

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def get(self, url: str, optional: bool = False) -> tuple[dict | None, bool]:
        """
        Fetch a Fitbit API endpoint, refreshing the token once on 401.

        Args:
            url:      Full Fitbit API URL.
            optional: If True, 404/400 responses are treated as "no data"
                      rather than errors.

        Returns:
            (data, rate_limited) where data is a dict or None.
        """
        data, rate_limited = self._request(url, self.access_token, optional)
        return data, rate_limited

    def get_device_info(self) -> dict:
        """
        Fetch device metadata (type, lastSyncTime) for this client's token,
        refreshing the token once on 401.

        Returns:
            First device dict with ``lastSyncTime`` parsed to a datetime.

        Raises:
            Exception: on any non-recoverable API error.
        """
        url = "https://api.fitbit.com/1/user/-/devices.json"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        resp = requests.get(url, headers=headers)

        if resp.status_code == 401:
            logger.warning("Token expired fetching device info, refreshing...")
            self._do_refresh()
            headers = {"Authorization": f"Bearer {self.access_token}"}
            resp = requests.get(url, headers=headers)

        if resp.status_code != 200:
            error_msg = f"Fitbit API request failed with status {resp.status_code}"
            try:
                error_data = resp.json()
                if "errors" in error_data:
                    error_msg += f": {error_data['errors']}"
            except Exception:
                if resp.text:
                    error_msg += f": {resp.text}"
            raise Exception(error_msg)

        device_data = resp.json()
        if not device_data:
            raise Exception("No devices found in response")

        first_device = device_data[0]
        try:
            first_device["lastSyncTime"] = datetime.strptime(
                first_device["lastSyncTime"], "%Y-%m-%dT%H:%M:%S.%f"
            )
        except ValueError as e:
            raise Exception(f"Failed to parse lastSyncTime: {e}")

        return first_device

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _request(
        self, url: str, token: str, optional: bool
    ) -> tuple[dict | None, bool]:
        """
        Execute a single GET request. On 401, refresh tokens and retry once.
        """
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers)

        if resp.status_code == 200:
            return resp.json(), False

        if resp.status_code == 429:
            return None, True

        if resp.status_code == 401:
            logger.warning(f"Token expired for request to {url}, refreshing...")
            self._do_refresh()
            # Retry once with the new token
            headers = {"Authorization": f"Bearer {self.access_token}"}
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                return resp.json(), False
            if resp.status_code == 429:
                return None, True

        if optional and resp.status_code in (404, 400):
            return None, False

        resp.raise_for_status()
        return None, False

    def _do_refresh(self) -> None:
        """
        Refresh the token pair and invoke the persistence callback.

        Raises:
            Exception: if the refresh request fails.
        """
        new_access, new_refresh = refresh_tokens(self.refresh_token)
        if not new_access or not new_refresh:
            raise Exception("Token refresh failed: no tokens returned.")

        self.access_token = new_access
        self.refresh_token = new_refresh
        logger.info("Token refreshed successfully.")

        if self.on_tokens_updated:
            try:
                self.on_tokens_updated(new_access, new_refresh)
            except Exception as e:
                logger.error(f"on_tokens_updated callback raised an error: {e}")
