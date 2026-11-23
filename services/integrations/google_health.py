"""
Google Health API client and OAuth helpers.

Exposes the same public interface as the provider client so callers can
fetch data from the Google Health API.  Accepts wearable data URLs and
translates them to the corresponding Google Health API calls, synthesizing
a Google Health API-shaped response.
"""

import logging
import re
import requests
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Optional

from config import (
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    GOOGLE_REDIRECT_URI,
    GOOGLE_AUTH_URL,
    GOOGLE_TOKEN_URL,
    HEALTH_API_BASE,
)

logger = logging.getLogger(__name__)

# Seconds before an HTTP call to Google is abandoned. Without a timeout a
# stalled connection would freeze a collector forever.
REQUEST_TIMEOUT = 30

# Largest page the dataPoints list/reconcile endpoints accept (sleep and
# exercise are capped at 25 by the API). Big pages keep request count low.
DATA_POINTS_PAGE_SIZE = 10000
SESSION_PAGE_SIZE = 25
# Guard against a provider bug returning the same page token forever.
MAX_DATA_POINT_PAGES = 200


class RefreshTokenRevokedError(Exception):
    """The refresh token is no longer valid (revoked, expired or unused for
    too long). The device must be authorized again by the participant."""

# ---------------------------------------------------------------------------
# Google Health API scopes (read-only — adjust if your app writes)
# ---------------------------------------------------------------------------
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly",
    "https://www.googleapis.com/auth/googlehealth.health_metrics_and_measurements.readonly",
    "https://www.googleapis.com/auth/googlehealth.sleep.readonly",
    "https://www.googleapis.com/auth/googlehealth.profile.readonly",
    "https://www.googleapis.com/auth/googlehealth.nutrition.readonly",
    "https://www.googleapis.com/auth/googlehealth.settings.readonly",
    "https://www.googleapis.com/auth/googlehealth.ecg.readonly",
    "https://www.googleapis.com/auth/googlehealth.irn.readonly",
]


# ===================================================================
# OAuth helpers
# ===================================================================

def generate_google_auth_url(state: str, code_challenge: str) -> str:
    """Build a Google OAuth 2.0 authorization URL (PKCE, S256)."""
    from urllib.parse import urlencode

    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(GOOGLE_SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    return f"{GOOGLE_AUTH_URL}?{urlencode(params)}"


def exchange_google_code(code: str, code_verifier: str = "") -> tuple[str | None, str | None]:
    """Exchange an authorization code for Google access/refresh tokens.

    ``code_verifier`` is the PKCE verifier stored with the pending
    authorization. It is omitted when empty, so links issued before PKCE
    was introduced still work.
    """
    payload = {
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "grant_type": "authorization_code",
    }
    if code_verifier:
        payload["code_verifier"] = code_verifier
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(GOOGLE_TOKEN_URL, data=payload, headers=headers, timeout=REQUEST_TIMEOUT)
    if resp.status_code != 200:
        raise Exception(f"Google token exchange failed: {resp.text}")
    tokens = resp.json()
    return tokens.get("access_token"), tokens.get("refresh_token")


def refresh_google_tokens(refresh_token: str) -> tuple[str | None, str | None]:
    """Refresh an expired Google access token."""
    payload = {
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(GOOGLE_TOKEN_URL, data=payload, headers=headers, timeout=REQUEST_TIMEOUT)
    if resp.status_code in (400, 401):
        try:
            error = resp.json().get("error")
        except ValueError:
            error = None
        if error == "invalid_grant":
            raise RefreshTokenRevokedError(f"Google refresh token is no longer valid: {resp.text}")
    if resp.status_code != 200:
        raise Exception(f"Google token refresh failed: {resp.text}")
    tokens = resp.json()
    return tokens.get("access_token"), tokens.get("refresh_token")


# ===================================================================
# URL pattern matching — route data URLs → Google Health API
# ===================================================================

# Pattern: .../activities/date/{date}.json
_RE_ACTIVITY_SUMMARY = re.compile(
    r"/activities/date/(\d{4}-\d{2}-\d{2})\.json$"
)
# Pattern: .../activities/heart/date/{date}/1d.json
_RE_HEART_DAILY = re.compile(
    r"/activities/heart/date/(\d{4}-\d{2}-\d{2})/1d\.json$"
)
# Pattern: .../activities/heart/date/{date}/1d/1min.json  (intraday)
_RE_HEART_INTRADAY = re.compile(
    r"/activities/heart/date/(\d{4}-\d{2}-\d{2})/1d/1min\.json$"
)
# Pattern: .../activities/{metric}/date/{date}/1d/1min.json  (steps, calories, …)
_RE_INTRADAY = re.compile(
    r"/activities/(steps|calories|distance|floors|elevation)/"
    r"date/(\d{4}-\d{2}-\d{2})/1d/1min\.json$"
)
# Pattern: .../sleep/date/{date}.json
_RE_SLEEP = re.compile(
    r"/sleep/date/(\d{4}-\d{2}-\d{2})\.json$"
)
# Pattern: .../foods/log/date/{date}.json
_RE_FOOD = re.compile(
    r"/foods/log/date/(\d{4}-\d{2}-\d{2})\.json$"
)
# Pattern: .../foods/log/water/date/{date}.json
_RE_WATER = re.compile(
    r"/foods/log/water/date/(\d{4}-\d{2}-\d{2})\.json$"
)
# Pattern: .../spo2/date/{date}.json  (daily)
_RE_SPO2_DAILY = re.compile(
    r"/spo2/date/(\d{4}-\d{2}-\d{2})\.json$"
)
# Pattern: .../spo2/date/{date}/all.json  (intraday)
_RE_SPO2_INTRADAY = re.compile(
    r"/spo2/date/(\d{4}-\d{2}-\d{2})/all\.json$"
)
# Pattern: .../br/date/{date}.json  (daily breathing rate)
_RE_BR_DAILY = re.compile(
    r"/br/date/(\d{4}-\d{2}-\d{2})\.json$"
)
# Pattern: .../br/date/{date}/all.json  (intraday)
_RE_BR_INTRADAY = re.compile(
    r"/br/date/(\d{4}-\d{2}-\d{2})/all\.json$"
)
# Pattern: .../hrv/date/{date}/all.json
_RE_HRV_INTRADAY = re.compile(
    r"/hrv/date/(\d{4}-\d{2}-\d{2})/all\.json$"
)
# Pattern: .../temp/core/date/{date}.json
_RE_TEMP_CORE = re.compile(
    r"/temp/core/date/(\d{4}-\d{2}-\d{2})\.json$"
)
# Pattern: .../devices.json
_RE_DEVICES = re.compile(r"/devices\.json$")


# ===================================================================
# Google Health API client
# ===================================================================

class GoogleHealthClient:
    """
    Stateful Google Health API client scoped to a single device.

    Exposes the same public interface as the provider client so callers
    can fetch data from the Google Health API.
    """

    def __init__(
        self,
        access_token: str,
        refresh_token: str,
        on_tokens_updated: Optional[Callable[[str, str], None]] = None,
        on_tokens_revoked: Optional[Callable[[], None]] = None,
    ):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.on_tokens_updated = on_tokens_updated
        self.on_tokens_revoked = on_tokens_revoked
        self._revoked = False

    # ------------------------------------------------------------------
    # Public interface  (mirrors the provider client)
    # ------------------------------------------------------------------

    def get(self, url: str, optional: bool = False) -> tuple[dict | None, bool]:
        """Route a wearable data URL → the appropriate Google Health API call."""
        if self._is_data_url(url):
            return self._handle_data_url(url, optional)
        return self._request(url, self.access_token, optional)

    def get_device_info(self) -> dict:
        """Fetch paired devices — returns the first paired device."""
        url = f"{HEALTH_API_BASE}/users/me/pairedDevices"
        headers = {"Authorization": f"Bearer {self.access_token}", "Accept": "application/json"}
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

        if resp.status_code == 401:
            logger.warning("Token expired fetching device info, refreshing…")
            self._do_refresh()
            headers = {"Authorization": f"Bearer {self.access_token}", "Accept": "application/json"}
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

        if resp.status_code != 200:
            raise Exception(
                f"Google Health API error {resp.status_code}: {resp.text}"
            )

        devices = resp.json().get("pairedDevices", [])
        if not devices:
            raise Exception("No paired devices found")

        d = devices[0]
        last_sync = d.get("lastSyncTime")
        if not last_sync:
            raise Exception("Paired device has no lastSyncTime")

        return {
            "deviceVersion": d.get("deviceVersion", "Unknown"),
            "lastSyncTime": datetime.fromisoformat(
                last_sync.replace("Z", "+00:00")
            ),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _request(
        self, url: str, token: str, optional: bool
    ) -> tuple[dict | None, bool]:
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

        if resp.status_code == 200:
            return resp.json(), False
        if resp.status_code == 429:
            return None, True
        if resp.status_code == 401:
            logger.warning(f"Token expired for {url}, refreshing…")
            self._do_refresh()
            headers = {"Authorization": f"Bearer {self.access_token}", "Accept": "application/json"}
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.json(), False
            if resp.status_code == 429:
                return None, True
        if optional and resp.status_code in (404, 400):
            return None, False
        resp.raise_for_status()
        return None, False

    def _do_refresh(self) -> None:
        if self._revoked:
            # Already known to be dead: don't ask Google again.
            raise RefreshTokenRevokedError("Google refresh token is no longer valid.")
        try:
            new_access, new_refresh = refresh_google_tokens(self.refresh_token)
        except RefreshTokenRevokedError:
            self._revoked = True
            logger.error("Google refresh token revoked or expired; re-authorization required.")
            if self.on_tokens_revoked:
                try:
                    self.on_tokens_revoked()
                except Exception as e:
                    logger.error(f"on_tokens_revoked callback failed: {e}")
            raise
        if not new_access:
            raise Exception("Google token refresh failed: no access token returned.")
        self.access_token = new_access
        if new_refresh:
            self.refresh_token = new_refresh
        logger.info("Google token refreshed successfully.")
        if self.on_tokens_updated:
            try:
                self.on_tokens_updated(new_access, self.refresh_token)
            except Exception as e:
                logger.error(f"on_tokens_updated callback failed: {e}")

    # ------------------------------------------------------------------
    # Data URL translation
    # ------------------------------------------------------------------

    @staticmethod
    def _is_data_url(url: str) -> bool:
        """Return True if the URL points at a wearable data endpoint."""
        return any(
            marker in url
            for marker in (
                "/activities/",
                "/sleep/",
                "/foods/",
                "/spo2/",
                "/br/",
                "/hrv/",
                "/temp/",
                "/devices",
            )
        )

    def _handle_data_url(
        self, url: str, optional: bool
    ) -> tuple[dict | None, bool]:
        """Route a wearable data URL → the appropriate Google Health API call."""

        # --- Daily activity summary ---
        m = _RE_ACTIVITY_SUMMARY.search(url)
        if m:
            return self._translate_activity_summary(m.group(1), optional)

        # --- Daily heart rate ---
        m = _RE_HEART_DAILY.search(url)
        if m:
            return self._translate_heart_daily(m.group(1), optional)

        # --- Intraday heart rate ---
        m = _RE_HEART_INTRADAY.search(url)
        if m:
            return self._translate_intraday_heart(m.group(1), optional)

        # --- Other intraday (steps, calories, distance, floors, elevation) ---
        m = _RE_INTRADAY.search(url)
        if m:
            return self._translate_intraday_metric(m.group(1), m.group(2), optional)

        # --- Sleep ---
        m = _RE_SLEEP.search(url)
        if m:
            return self._translate_sleep(m.group(1), optional)

        # --- Food log ---
        m = _RE_FOOD.search(url)
        if m:
            return self._translate_food(m.group(1), optional)

        # --- Water ---
        m = _RE_WATER.search(url)
        if m:
            return self._translate_water(m.group(1), optional)

        # --- SpO2 daily ---
        m = _RE_SPO2_DAILY.search(url)
        if m:
            return self._translate_spo2_daily(m.group(1), optional)

        # --- SpO2 intraday ---
        m = _RE_SPO2_INTRADAY.search(url)
        if m:
            return self._translate_spo2_intraday(m.group(1), optional)

        # --- Breathing rate daily ---
        m = _RE_BR_DAILY.search(url)
        if m:
            return self._translate_br_daily(m.group(1), optional)

        # --- Breathing rate intraday ---
        m = _RE_BR_INTRADAY.search(url)
        if m:
            return self._translate_br_intraday(m.group(1), optional)

        # --- HRV intraday ---
        m = _RE_HRV_INTRADAY.search(url)
        if m:
            return self._translate_hrv_intraday(m.group(1), optional)

        # --- Core temperature ---
        m = _RE_TEMP_CORE.search(url)
        if m:
            return self._translate_temperature(m.group(1), optional)

        # --- Device info ---
        m = _RE_DEVICES.search(url)
        if m:
            return self._translate_devices(optional)

        # Fallback: try raw GET
        logger.warning(f"Unrecognised data URL pattern, falling back: {url}")
        return self._request(url, self.access_token, optional)

    # ------------------------------------------------------------------
    # POST helper for dailyRollUp
    # ------------------------------------------------------------------

    def _daily_rollup(
        self, data_type: str, date_str: str, optional: bool = False
    ) -> tuple[dict | None, bool]:
        """Call the Google Health API dailyRollUp endpoint and return the raw response."""
        url = f"{HEALTH_API_BASE}/users/me/dataTypes/{data_type}/dataPoints:dailyRollUp"

        year, month, day = date_str.split("-")
        body = {
            "range": {
                "start": {
                    "date": {"year": int(year), "month": int(month), "day": int(day)},
                    "time": {"hours": 0, "minutes": 0, "seconds": 0, "nanos": 0},
                },
                "end": {
                    "date": {"year": int(year), "month": int(month), "day": int(day)},
                    "time": {"hours": 23, "minutes": 59, "seconds": 59, "nanos": 0},
                },
            },
            "windowSizeDays": 1,
        }

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        resp = requests.post(url, json=body, headers=headers, timeout=REQUEST_TIMEOUT)

        if resp.status_code == 429:
            return None, True
        if optional and resp.status_code in (404, 400):
            return None, False
        if resp.status_code == 401:
            self._do_refresh()
            headers["Authorization"] = f"Bearer {self.access_token}"
            resp = requests.post(url, json=body, headers=headers, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.json(), False
        if optional:
            return None, False
        resp.raise_for_status()
        return None, False

    @staticmethod
    def _civil_day_filter(data_type: str, date_str: str, sample: bool) -> str:
        """Filter matching exactly one local (civil) day: [date_str, date_str + 1 day).

        Both bounds are required: the API returns points newest-first, so a
        lower bound alone yields the most recent points, not the requested day.
        """
        field = data_type.replace("-", "_")
        time_field = "sample_time.civil_time" if sample else "interval.civil_start_time"
        next_day = (date.fromisoformat(date_str) + timedelta(days=1)).isoformat()
        return (
            f'{field}.{time_field} >= "{date_str}" AND '
            f'{field}.{time_field} < "{next_day}"'
        )

    def _list_data_points(
        self,
        data_type: str,
        date_str: str,
        optional: bool = False,
        filter_expr: str | None = None,
        sample: bool = False,
        action: str = "list",
    ) -> tuple[dict | None, bool]:
        """Call the Google Health API list (or reconcile) endpoint for one day.

        Unless `filter_expr` is given, the filter selects the local day
        `date_str`, using the sample time for sample data types (heart rate,
        SpO2, HRV, …) and the interval start for interval types (steps, …).

        Follows `nextPageToken` and returns every page merged into a single
        ``{"dataPoints": [...]}`` response.
        """
        url = f"{HEALTH_API_BASE}/users/me/dataTypes/{data_type}/dataPoints"
        if action == "reconcile":
            url = url + ":reconcile"

        params = {
            "filter": filter_expr or self._civil_day_filter(data_type, date_str, sample),
            "pageSize": SESSION_PAGE_SIZE if data_type in ("sleep", "exercise") else DATA_POINTS_PAGE_SIZE,
        }

        data_points: list = []
        for page in range(MAX_DATA_POINT_PAGES):
            # Only the first page may be "optionally" missing: a failure after
            # that would silently drop data, so it must raise.
            resp_json, rate_limited = self._get_data_points_page(
                url, params, optional=optional and page == 0
            )
            if rate_limited:
                return None, True
            if resp_json is None:
                return None, False

            data_points.extend(resp_json.get("dataPoints", []))

            next_token = resp_json.get("nextPageToken")
            if not next_token:
                return {"dataPoints": data_points}, False
            params["pageToken"] = next_token

        raise Exception(
            f"Too many result pages for {data_type} on {date_str} "
            f"(more than {MAX_DATA_POINT_PAGES})"
        )

    def _get_data_points_page(
        self, url: str, params: dict, optional: bool
    ) -> tuple[dict | None, bool]:
        """Fetch a single page of a list/reconcile call."""
        headers = {"Authorization": f"Bearer {self.access_token}", "Accept": "application/json"}
        resp = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)

        if resp.status_code == 429:
            return None, True
        if optional and resp.status_code in (404, 400):
            return None, False
        if resp.status_code == 401:
            self._do_refresh()
            headers["Authorization"] = f"Bearer {self.access_token}"
            resp = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.json(), False
        if optional:
            return None, False
        resp.raise_for_status()
        return None, False

    # ------------------------------------------------------------------
    # Translation methods — each one synthesises a Google Health API-shaped response
    # ------------------------------------------------------------------

    def _translate_activity_summary(
        self, date_str: str, optional: bool
    ) -> tuple[dict | None, bool]:
        data = {"summary": {}}

        # steps
        resp, limited = self._daily_rollup("steps", date_str, optional=True)
        if limited:
            return None, True
        if resp:
            val = self._extract_rollup_int(resp, "steps", "countSum")
            data["summary"]["steps"] = val

        # active energy burned (caloriesOut)
        resp, limited = self._daily_rollup("active-energy-burned", date_str, optional=True)
        if limited:
            return None, True
        if resp:
            val = self._extract_rollup_float(resp, "active-energy-burned", "kilocaloriesSum")
            data["summary"]["caloriesOut"] = val

        # total calories
        resp, limited = self._daily_rollup("total-calories", date_str, optional=True)
        if limited:
            return None, True
        if resp:
            val = self._extract_rollup_float(resp, "total-calories", "kilocaloriesSum")
            data["summary"]["caloriesOut"] = val

        # floors
        resp, limited = self._daily_rollup("floors", date_str, optional=True)
        if limited:
            return None, True
        if resp:
            val = self._extract_rollup_int(resp, "floors", "countSum")
            data["summary"]["floors"] = val

        # altitude (elevation)
        resp, limited = self._daily_rollup("altitude", date_str, optional=True)
        if limited:
            return None, True
        if resp:
            val = self._extract_rollup_float(resp, "altitude", "metersSum")
            data["summary"]["elevation"] = val

        # distance
        resp, limited = self._daily_rollup("distance", date_str, optional=True)
        if limited:
            return None, True
        if resp:
            val = self._extract_rollup_float(resp, "distance", "metersSum")
            data["summary"]["distances"] = [{"distance": val}]

        # activity level → active / sedentary minutes
        resp, limited = self._daily_rollup("activity-level", date_str, optional=True)
        if limited:
            return None, True
        if resp:
            v_active, v_sedentary = self._extract_activity_minutes(resp)
            data["summary"]["veryActiveMinutes"] = v_active
            data["summary"]["sedentaryMinutes"] = v_sedentary

        return (data if data["summary"] else None), False

    def _translate_heart_daily(
        self, date_str: str, optional: bool
    ) -> tuple[dict | None, bool]:
        resp, limited = self._daily_rollup("daily-resting-heart-rate", date_str, optional=True)
        if limited:
            return None, True
        hrm = 0
        if resp:
            hrm = self._extract_rollup_float(resp, "daily-resting-heart-rate", "bpmAverage")
        return {"activities-heart": [{"value": {"restingHeartRate": hrm}}]}, False

    def _translate_intraday_heart(
        self, date_str: str, optional: bool
    ) -> tuple[dict | None, bool]:
        resp, limited = self._list_data_points("heart-rate", date_str, optional=True, sample=True)
        if limited:
            return None, True
        dataset = self._to_intraday_dataset(resp, "heart-rate", "beatsPerMinute")
        return {"activities-heart-intraday": {"dataset": dataset}}, False

    def _translate_intraday_metric(
        self, metric: str, date_str: str, optional: bool
    ) -> tuple[dict | None, bool]:
        dt_map = {
            "steps": ("steps", "count"),
            "calories": ("active-energy-burned", "kilocalories"),
            "distance": ("distance", "meters"),
            "floors": ("floors", "count"),
            "elevation": ("altitude", "meters"),
        }
        gtype, field = dt_map.get(metric, (metric, "floatVal"))
        # Google does not support the `list` action for all data types
        # (e.g. `floors` only supports reconcile / rollup / dailyRollUp).
        action = "reconcile" if gtype == "floors" else "list"
        resp, limited = self._list_data_points(gtype, date_str, optional=True, action=action)
        if limited:
            return None, True
        dataset = self._to_intraday_dataset(resp, gtype, field)
        key = f"activities-{metric}-intraday"
        return {key: {"dataset": dataset}}, False

    def _translate_sleep(
        self, date_str: str, optional: bool
    ) -> tuple[dict | None, bool]:
        # Sessions that END on the requested local day (the night before it, plus naps).
        next_day = (date.fromisoformat(date_str) + timedelta(days=1)).isoformat()
        resp, limited = self._list_data_points(
            "sleep", date_str, optional=True,
            filter_expr=(
                f'sleep.interval.civil_end_time >= "{date_str}" AND '
                f'sleep.interval.civil_end_time < "{next_day}"'
            ),
        )
        if limited:
            return None, True
        if not resp:
            return {"sleep": []}, False

        level_map = {
            "AWAKE": "wake",
            "LIGHT": "light",
            "DEEP": "deep",
            "REM": "rem",
            "RESTLESS": "restless",
            "ASLEEP": "asleep",
        }

        sleep_logs = []
        for dp in resp.get("dataPoints", []):
            s = dp.get("sleep", {})
            summary = s.get("summary", {})
            metadata = s.get("metadata", {})
            interval = s.get("interval", {})
            minutes_in_bed = int(summary.get("minutesInSleepPeriod", 0))
            log = {
                "dateOfSleep": date_str,
                "duration": minutes_in_bed * 60 * 1000,
                "minutesAsleep": int(summary.get("minutesAsleep", 0)),
                "minutesAwake": int(summary.get("minutesAwake", 0)),
                "timeInBed": minutes_in_bed,
                "startTime": interval.get("startTime", ""),
                "endTime": interval.get("endTime", ""),
                "isMainSleep": metadata.get("main", False),
                "logType": "manual" if metadata.get("manuallyEdited") else "auto_detected",
                "type": "stages" if s.get("type") == "STAGES" else "classic",
                "levels": {},
            }

            stages = s.get("stages", [])
            if stages:
                log["levels"]["data"] = [
                    {
                        "dateTime": st.get("startTime", ""),
                        "level": level_map.get(
                            st.get("type", "").upper(), st.get("type", "").lower()
                        ),
                        "seconds": self._duration_seconds(
                            st.get("startTime"), st.get("endTime")
                        ),
                    }
                    for st in stages
                ]

            short_awakenings = s.get("shortAwakenings", [])
            if short_awakenings:
                log["levels"]["shortData"] = [
                    {
                        "dateTime": st.get("startTime", ""),
                        "level": "wake",
                        "seconds": self._duration_seconds(
                            st.get("startTime"), st.get("endTime")
                        ),
                    }
                    for st in short_awakenings
                ]

            sleep_logs.append(log)

        return {"sleep": sleep_logs}, False

    def _translate_food(
        self, date_str: str, optional: bool
    ) -> tuple[dict | None, bool]:
        resp, limited = self._daily_rollup("nutrition-log", date_str, optional=True)
        if limited:
            return None, True
        cal = 0
        if resp:
            cal = self._extract_rollup_float(resp, "nutrition-log", "kilocaloriesSum")
        return {"summary": {"calories": cal}}, False

    def _translate_water(
        self, date_str: str, optional: bool
    ) -> tuple[dict | None, bool]:
        return {"summary": {"water": 0}}, False

    def _translate_spo2_daily(
        self, date_str: str, optional: bool
    ) -> tuple[dict | None, bool]:
        resp, limited = self._daily_rollup("daily-oxygen-saturation", date_str, optional=True)
        if limited:
            return None, True
        avg = 0.0
        if resp:
            avg = self._extract_rollup_float(resp, "daily-oxygen-saturation", "percentageAvg")
            if avg == 0.0:
                avg = self._extract_rollup_float(resp, "daily-oxygen-saturation", "percentageAverage")

        result = {"value": {"avg": avg}} if avg else {"value": 0}
        return result, False

    def _translate_spo2_intraday(
        self, date_str: str, optional: bool
    ) -> tuple[dict | None, bool]:
        resp, limited = self._list_data_points("oxygen-saturation", date_str, optional=True, sample=True)
        if limited:
            return None, True
        if not resp:
            return None, False
        minutes = []
        for dp in resp.get("dataPoints", []):
            osat = dp.get("oxygen-saturation", dp.get("oxygenSaturation", {}))
            ts = osat.get("sampleTime", {}).get("physicalTime") or dp.get("startTime", "")
            val = osat.get("percentage", 0)
            if ts:
                minutes.append({"minute": ts, "value": float(val)})
        return {"minutes": minutes}, False

    def _translate_br_daily(
        self, date_str: str, optional: bool
    ) -> tuple[dict | None, bool]:
        return {"value": {"breathingRate": 0}}, False

    def _translate_br_intraday(
        self, date_str: str, optional: bool
    ) -> tuple[dict | None, bool]:
        return None, False

    def _translate_hrv_intraday(
        self, date_str: str, optional: bool
    ) -> tuple[dict | None, bool]:
        resp, limited = self._list_data_points("heart-rate-variability", date_str, optional=True, sample=True)
        if limited:
            return None, True
        if not resp:
            return None, False
        minutes = []
        for dp in resp.get("dataPoints", []):
            hrv = dp.get("heart-rate-variability", dp.get("heartRateVariability", {}))
            ts = hrv.get("sampleTime", {}).get("physicalTime") or dp.get("startTime", "")
            rmssd = hrv.get("rmssd", {}).get("milliseconds", 0)
            lf_val = hrv.get("lfPower", {})
            hf_val = hrv.get("hfPower", {})
            cov = hrv.get("coverage", {})
            minutes.append({
                "dateTime": ts,
                "minute": ts,
                "value": {
                    "rmssd": float(rmssd),
                    "lf": float(lf_val.get("milliseconds", 0) if isinstance(lf_val, dict) else lf_val),
                    "hf": float(hf_val.get("milliseconds", 0) if isinstance(hf_val, dict) else hf_val),
                    "coverage": float(cov.get("percentage", 0) if isinstance(cov, dict) else cov),
                }
            })
        return {"minutes": minutes}, False

    def _translate_temperature(
        self, date_str: str, optional: bool
    ) -> tuple[dict | None, bool]:
        resp, limited = self._daily_rollup("core-body-temperature", date_str, optional=True)
        if limited:
            return None, True
        val = 0.0
        if resp:
            val = self._extract_rollup_float(resp, "core-body-temperature", "celsiusAvg")
            if val == 0.0:
                val = self._extract_rollup_float(resp, "core-body-temperature", "celsiusAverage")
        return {"value": val}, False

    def _translate_devices(
        self, optional: bool = False
    ) -> tuple[dict | None, bool]:
        try:
            info = self.get_device_info()
            return [
                {
                    "deviceVersion": info["deviceVersion"],
                    "lastSyncTime": info["lastSyncTime"].strftime("%Y-%m-%dT%H:%M:%S.%f"),
                }
            ], False
        except Exception:
            if optional:
                return None, False
            raise

    # ------------------------------------------------------------------
    # Response extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_rollup_int(resp: dict, data_type: str, field: str) -> int:
        dps = resp.get("rollupDataPoints", [])
        if not dps:
            return 0
        dp = dps[0]
        obj = dp.get(data_type.replace("-", "_"), dp.get(data_type, {}))
        if isinstance(obj, dict):
            return int(obj.get(field, 0))
        return int(obj or 0)

    @staticmethod
    def _extract_rollup_float(resp: dict, data_type: str, field: str) -> float:
        dps = resp.get("rollupDataPoints", [])
        if not dps:
            return 0.0
        dp = dps[0]
        key = data_type.replace("-", "_")
        obj = dp.get(key, dp.get(data_type, {}))
        if isinstance(obj, dict):
            return float(obj.get(field, 0))
        return float(obj or 0)

    @staticmethod
    def _extract_activity_minutes(resp: dict) -> tuple[int, int]:
        very_active = 0
        sedentary = 0
        for dp in resp.get("rollupDataPoints", []):
            al = dp.get("activity_level", dp.get("activityLevel", {}))
            if isinstance(al, dict):
                very_active += int(al.get("veryActiveMinutes", 0))
                sedentary += int(al.get("sedentaryMinutes", 0))
        return very_active, sedentary

    @staticmethod
    def _to_intraday_dataset(
        resp: dict | None, data_type: str, value_field: str
    ) -> list[dict]:
        """Convert Google Health API list response → intraday dataset.

        Google data points nest the measurement under a camelCase data-type
        key (e.g. ``steps``, ``heartRate``) which wraps either an ``interval``
        (interval types) or a ``sampleTime`` (sample types).
        """
        dataset = []
        if not resp:
            return dataset
        for dp in resp.get("dataPoints", []):
            obj = None
            target = data_type.replace("-", "").lower()
            for key, val in dp.items():
                if isinstance(val, dict) and key.lower().replace("_", "") == target:
                    obj = val
                    break
            if not obj:
                continue

            dt = GoogleHealthClient._local_datetime(obj)
            if dt is None:
                continue

            val = obj.get(value_field)
            try:
                val = float(str(val)) if val is not None else 0.0
            except (TypeError, ValueError):
                val = 0.0

            dataset.append({
                "datetime": dt,
                "time": dt.strftime("%H:%M:%S"),
                "value": int(val) if val.is_integer() else val,
            })
        return dataset

    @staticmethod
    def _local_datetime(obj: dict) -> datetime | None:
        """Wearer's local (civil) date and time of a data point, as a naive datetime.

        Uses the API's civil time when present; otherwise the physical (UTC)
        time shifted by the recorded UTC offset; as a last resort the UTC time.
        """
        interval = obj.get("interval") or {}
        sample_time = obj.get("sampleTime") or {}

        civil = interval.get("civilStartTime") or sample_time.get("civilTime")
        if civil:
            d = civil.get("date") or {}
            t = civil.get("time") or {}
            try:
                # Proto JSON omits zero-valued fields (e.g. minutes at :00)
                return datetime(
                    int(d["year"]), int(d["month"]), int(d["day"]),
                    int(t.get("hours", 0)), int(t.get("minutes", 0)), int(t.get("seconds", 0)),
                )
            except (KeyError, TypeError, ValueError):
                pass

        ts = interval.get("startTime") or sample_time.get("physicalTime")
        if not ts:
            return None
        try:
            physical = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except ValueError:
            return None
        if physical.tzinfo is None:
            physical = physical.replace(tzinfo=timezone.utc)

        offset = interval.get("startUtcOffset") or sample_time.get("utcOffset")
        try:
            offset_s = float(str(offset).rstrip("s")) if offset else 0.0
        except ValueError:
            offset_s = 0.0

        local = physical.astimezone(timezone.utc) + timedelta(seconds=offset_s)
        return local.replace(tzinfo=None)

    @staticmethod
    def _duration_seconds(start: str, end: str) -> int:
        try:
            s = datetime.fromisoformat(start.replace("Z", "+00:00"))
            e = datetime.fromisoformat(end.replace("Z", "+00:00"))
            return int((e - s).total_seconds())
        except Exception:
            return 0
