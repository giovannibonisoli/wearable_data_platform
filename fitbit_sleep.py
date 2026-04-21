"""
FITBIT SLEEP COLLECTOR

Runs continuously in background. Delegates to FitbitSleepCollectorService.
"""

import time
import logging

from dotenv import load_dotenv

load_dotenv()

from database import ConnectionManager
from services.collectors.fitbit_sleep_collector import FitbitSleepCollectorService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/fitbit_summary.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

CYCLE_SLEEP_SECONDS = 1800  # 30 minutes when at least one device processed
RATE_LIMIT_SLEEP_SECONDS = 600  # 10 minutes when all devices rate-limited
NO_DEVICES_SLEEP_SECONDS = 60


def main_loop():
    logger.info("=== FITBIT SLEEP DATA COLLECTOR STARTED ===")
    while True:
        try:
            with ConnectionManager() as conn:
                service = FitbitSleepCollectorService(conn)
                results = service.collect_for_all_devices()

            total = sum(results.values())
            if total == 0:
                logger.warning("No devices found")
                time.sleep(NO_DEVICES_SLEEP_SECONDS)
                continue

            logger.info(
                f"Cycle complete: {results['success']} successful, "
                f"{results['rate_limited']} rate-limited, {results['error']} errors"
            )

            if results["rate_limited"] == total and results["rate_limited"] > 0:
                logger.info("ALL devices rate-limited. Sleeping 10 minutes.")
                time.sleep(RATE_LIMIT_SLEEP_SECONDS)
            else:
                logger.info("At least one device processed. Sleeping 30 minutes.")
                time.sleep(CYCLE_SLEEP_SECONDS)

        except KeyboardInterrupt:
            logger.info("=== STOPPED BY USER ===")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
            time.sleep(NO_DEVICES_SLEEP_SECONDS)


if __name__ == "__main__":
    main_loop()
