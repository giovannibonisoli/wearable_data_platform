"""
Google Health API client and OAuth helpers.

Mirrors the ``FitbitClient`` interface so callers can switch providers
transparently.  Accepts Fitbit‑style URLs and translates them to the
corresponding Google Health API calls, synthesizing a Fitbit‑shaped response.
"""

import logging
import re
import requests
from datetime import datetime
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

def generate_google_auth_url(state: str) -> str:
    """Build a Google OAuth 2.0 authorization URL."""
    from urllib.parse import urlencode

    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(GOOGLE_SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    }
    return f"{GOOGLE_AUTH_URL}?{urlencode(params)}"


def exchange_google_code(code: str) -> tuple[str | None, str | None]:
    """Exchange an authorization code for Google access/refresh tokens."""
    payload = {
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "grant_type": "authorization_code",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(GOOGLE_TOKEN_URL, data=payload, headers=headers)
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
    resp = requests.post(GOOGLE_TOKEN_URL, data=payload, headers=headers)
    if resp.status_code != 200:
        raise Exception(f"Google token refresh failed: {resp.text}")
    tokens = resp.json()
    return tokens.get("access_token"), tokens.get("refresh_token")


# ===================================================================
# URL pattern matching — translate Fitbit URLs → Google Health API
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

    Exposes the same public interface as ``FitbitClient`` so callers
    remain unchanged when the provider is switched.
    """

    def __init__(
        self,
        access_token: str,
        refresh_token: str,
        on_tokens_updated: Optional[Callable[[str, str], None]] = None,
    ):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.on_tokens_updated = on_tokens_updated

    # ------------------------------------------------------------------
    # Public interface  (mirrors FitbitClient)
    # ------------------------------------------------------------------

    def get(self, url: str, optional: bool = False) -> tuple[dict | None, bool]:
        """Translate a Fitbit‑style URL → Google Health API call."""
        if self._is_fitbit_url(url):
            return self._handle_fitbit_url(url, optional)
        return self._request(url, self.access_token, optional)

    def get_device_info(self) -> dict:
        """Fetch paired devices — returns first device in Fitbit shape."""
        url = f"{HEALTH_API_BASE}/users/me/pairedDevices"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        resp = requests.get(url, headers=headers)

        if resp.status_code == 401:
            logger.warning("Token expired fetching device info, refreshing…")
            self._do_refresh()
            headers = {"Authorization": f"Bearer {self.access_token}"}
            resp = requests.get(url, headers=headers)

        if resp.status_code != 200:
            raise Exception(
                f"Google Health API error {resp.status_code}: {resp.text}"
            )

        devices = resp.json().get("devices", [])
        if not devices:
            raise Exception("No paired devices found")

        d = devices[0]
        return {
            "deviceVersion": d.get("displayName", "Unknown"),
            "lastSyncTime": datetime.fromisoformat(
                d.get("lastSyncedTime", "").replace("Z", "+00:00")
            ),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _request(
        self, url: str, token: str, optional: bool
    ) -> tuple[dict | None, bool]:
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers)

        if resp.status_code == 200:
            return resp.json(), False
        if resp.status_code == 429:
            return None, True
        if resp.status_code == 401:
            logger.warning(f"Token expired for {url}, refreshing…")
            self._do_refresh()
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
        new_access, new_refresh = refresh_google_tokens(self.refresh_token)
        if not new_access or not new_refresh:
            raise Exception("Google token refresh failed: no tokens returned.")
        self.access_token = new_access
        self.refresh_token = new_refresh
        logger.info("Google token refreshed successfully.")
        if self.on_tokens_updated:
            try:
                self.on_tokens_updated(new_access, new_refresh)
            except Exception as e:
                logger.error(f"on_tokens_updated callback failed: {e}")

    # ------------------------------------------------------------------
    # Fitbit URL translation
    # ------------------------------------------------------------------

    @staticmethod
    def _is_fitbit_url(url: str) -> bool:
        return "api.fitbit.com" in url

    def _handle_fitbit_url(
        self, url: str, optional: bool
    ) -> tuple[dict | None, bool]:
        """Route a Fitbit‑style URL → the appropriate Google Health API call."""

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
        logger.warning(f"Unrecognised Fitbit URL pattern, falling back: {url}")
        google_url = url.replace("https://api.fitbit.com/1/user/-", HEALTH_API_BASE + "/users/me")
        google_url = google_url.replace("https://api.fitbit.com/1.2/user/-", HEALTH_API_BASE + "/users/me")
        return self._request(google_url, self.access_token, optional)

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
            "Content-Type": "application/json",
        }
        resp = requests.post(url, json=body, headers=headers)

        if resp.status_code == 429:
            return None, True
        if optional and resp.status_code in (404, 400):
            return None, False
        if resp.status_code == 401:
            self._do_refresh()
            headers["Authorization"] = f"Bearer {self.access_token}"
            resp = requests.post(url, json=body, headers=headers)
        if resp.status_code == 200:
            return resp.json(), False
        if optional:
            return None, False
        resp.raise_for_status()
        return None, False

    def _list_data_points(
        self,
        data_type: str,
        date_str: str,
        optional: bool = False,
        filter_expr: str | None = None,
    ) -> tuple[dict | None, bool]:
        """Call the Google Health API list endpoint."""
        url = f"{HEALTH_API_BASE}/users/me/dataTypes/{data_type}/dataPoints"
        params = {}
        if filter_expr:
            params["filter"] = filter_expr
        else:
            params["filter"] = (
                f'{data_type.replace("-", "_")}.interval.civil_start_time >= "{date_str}T00:00:00"'
            )

        headers = {"Authorization": f"Bearer {self.access_token}"}
        resp = requests.get(url, headers=headers, params=params)

        if resp.status_code == 429:
            return None, True
        if optional and resp.status_code in (404, 400):
            return None, False
        if resp.status_code == 401:
            self._do_refresh()
            headers["Authorization"] = f"Bearer {self.access_token}"
            resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 200:
            return resp.json(), False
        if optional:
            return None, False
        resp.raise_for_status()
        return None, False

    # ------------------------------------------------------------------
    # Translation methods — each one synthesises a Fitbit‑shaped response
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
        resp, limited = self._list_data_points("heart-rate", date_str, optional=True)
        if limited:
            return None, True
        dataset = self._to_fitbit_dataset(resp, "heart-rate", "bpm")
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
        resp, limited = self._list_data_points(gtype, date_str, optional=True)
        if limited:
            return None, True
        dataset = self._to_fitbit_dataset(resp, gtype, field)
        key = f"activities-{metric}-intraday"
        return {key: {"dataset": dataset}}, False

    def _translate_sleep(
        self, date_str: str, optional: bool
    ) -> tuple[dict | None, bool]:
        resp, limited = self._list_data_points("sleep", date_str, optional=True,
                                                filter_expr=f'sleep.interval.civil_end_time >= "{date_str}"')
        if limited:
            return None, True
        if not resp:
            return {"sleep": []}, False

        sleep_logs = []
        for dp in resp.get("dataPoints", []):
            s = dp.get("sleep", {})
            summary = s.get("summary", {})
            metadata = s.get("metadata", {})
            total_asleep = int(summary.get("minutesAsleep", 0))
            log = {
                "dateOfSleep": date_str,
                "minutesAsleep": total_asleep,
                "minutesAwake": int(summary.get("minutesAwake", 0)),
                "minutesInTheBed": int(summary.get("minutesInSleepPeriod", 0)),
                "startTime": s.get("interval", {}).get("startTime", ""),
                "endTime": s.get("interval", {}).get("endTime", ""),
                "isMainSleep": metadata.get("main", False),
                "type": "stages" if s.get("type") == "STAGES" else "classic",
                "levels": {},
            }
            stages = s.get("stages", [])
            if stages:
                level_data = []
                for st in stages:
                    level_data.append({
                        "dateTime": st.get("startTime", ""),
                        "level": st.get("type", "").lower(),
                        "seconds": self._duration_seconds(
                            st.get("startTime"), st.get("endTime")
                        ),
                    })
                    short_data = []
                log["levels"]["data"] = level_data
                log["levels"]["shortData"] = level_data

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
        resp, limited = self._list_data_points("oxygen-saturation", date_str, optional=True)
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
        resp, limited = self._list_data_points("heart-rate-variability", date_str, optional=True)
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
    def _to_fitbit_dataset(
        resp: dict | None, data_type: str, value_field: str
    ) -> list[dict]:
        """Convert Google Health API list response → Fitbit intraday dataset."""
        dataset = []
        if not resp:
            return dataset
        for dp in resp.get("dataPoints", []):
            ts = dp.get("startTime", "")
            if ts:
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    time_str = dt.strftime("%H:%M:%S")
                except ValueError:
                    continue

                obj = dp.get(data_type.replace("-", "_"), dp.get(data_type, {}))
                val = 0
                if isinstance(obj, dict):
                    val = obj.get(value_field, 0)
                elif isinstance(obj, (int, float)):
                    val = obj
                else:
                    val = obj or 0

                dataset.append({"time": time_str, "value": int(val) if isinstance(val, (int, float)) and not isinstance(val, bool) else val})
        return dataset

    @staticmethod
    def _duration_seconds(start: str, end: str) -> int:
        try:
            s = datetime.fromisoformat(start.replace("Z", "+00:00"))
            e = datetime.fromisoformat(end.replace("Z", "+00:00"))
            return int((e - s).total_seconds())
        except Exception:
            return 0
