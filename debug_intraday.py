"""
Standalone debug script: fetch Google Health intraday data for a device/date.

Bypasses the collector logic and hits the Google Health API directly through
the provider client, printing raw responses so we can see whether intraday
data exists for a given day.

Uses the real Google Health API endpoint template:

    GET https://health.googleapis.com/v4/users/me/dataTypes/{dataType}/dataPoints

Usage:
    python debug_intraday.py --device-id <device_id> --date YYYY-MM-DD
"""

import argparse
import json
from datetime import datetime

from config import HEALTH_API_BASE
from database import ConnectionManager, DeviceRepository
from services.integrations.oauth_provider import create_api_client


# (label, google dataType, valueField, translated key, kind)
# kind: "interval" -> filtered via interval civil start time
#       "sample"  -> filtered via startTime/endTime time range
INTRADAY_METRICS = [
    ("heart_rate", "heart-rate", "beatsPerMinute", "activities-heart-intraday", "sample"),
    ("steps", "steps", "count", "activities-steps-intraday", "interval"),
    # ("calories", "active-energy-burned", "kilocalories", "activities-calories-intraday", "interval"),
    # ("distance", "distance", "meters", "activities-distance-intraday", "interval"),
    # ("floors", "floors", "count", "activities-floors-intraday", "interval"),
    # ("elevation", "altitude", "meters", "activities-elevation-intraday", "interval"),
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Debug Google Health intraday data")
    parser.add_argument("--device-id", type=int, required=True, help="Device ID")
    parser.add_argument("--date", type=str, default=datetime.now().strftime("%Y-%m-%d"), help="Date in YYYY-MM-DD")
    args = parser.parse_args()

    date_str = args.date

    with ConnectionManager() as conn:
        device_repo = DeviceRepository(conn)
        device = device_repo.get_by_id(args.device_id)
        if not device:
            raise SystemExit(f"Device {args.device_id} not found")

        access_token, refresh_token = device_repo.get_tokens(args.device_id)
        if not access_token or not refresh_token:
            raise SystemExit(f"No tokens found for device {args.device_id}")

        client = create_api_client(
            access_token=access_token,
            refresh_token=refresh_token,
            on_tokens_updated=lambda a, r: device_repo.update_tokens(args.device_id, a, r),
        )

        print(f"Device {args.device_id} ({device.email_address}) - date {date_str}")
        device_info = client.get_device_info()
        print(f"Device info: {device_info}")

        for data_type, gtype, value_field, key, kind in INTRADAY_METRICS:
            url = f"{HEALTH_API_BASE}/users/me/dataTypes/{gtype}/dataPoints"
            try:
                if kind == "sample":
                    start_time = f"{date_str}T00:00:00Z"
                    end_time = f"{date_str}T23:59:59Z"
                    resp, rate_limited = client._list_data_points(
                        gtype, date_str, optional=False, start_time=start_time, end_time=end_time
                    )
                else:
                    filter_expr = f'{gtype.replace("-", "_")}.interval.civil_start_time >= "{date_str}T00:00:00"'
                    action = "reconcile" if gtype == "floors" else "list"
                    resp, rate_limited = client._list_data_points(
                        gtype, date_str, optional=False, filter_expr=filter_expr, action=action
                    )

                dataset = client._to_intraday_dataset(resp, gtype, value_field)

                print(f"\n=== {data_type} ===")
                print(f"Endpoint: {url}")
                print(f"rate_limited: {rate_limited}")
                print(f"raw data: {json.dumps(resp, default=str, indent=2)}")
                print(f"dataset: {dataset}")
            except Exception as e:
                print(f"\n=== {data_type} ===")
                print(f"Endpoint: {url}")
                print(f"ERROR: {e}")
                if hasattr(e, "response") and e.response is not None:
                    print(f"HTTP status: {e.response.status_code}")
                    print(f"HTTP body: {e.response.text}")

if __name__ == "__main__":
    main()
