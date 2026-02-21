"""
FITBIT INTRADAY COLLECTOR

Runs continuously in background. Delegates to FitbitIntradayCollectorService.
"""

import os
import sys
import time
import logging
from dotenv import load_dotenv

from database import ConnectionManager
from services.collectors.fitbit_intraday_collector import FitbitIntradayCollectorService

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/fitbit_intraday_debug.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

CYCLE_SLEEP_SECONDS = 2  # Continue quickly when making progress
RATE_LIMIT_SLEEP_SECONDS = 600  # 10 minutes when all devices rate-limited
UP_TO_DATE_SLEEP_SECONDS = 600  # 10 minutes when all devices up to date
NO_DEVICES_SLEEP_SECONDS = 60


def main_loop():
    logger.info("=== FITBIT INTRADAY COLLECTOR STARTED ===")
    while True:
        try:
            with ConnectionManager() as conn:
                service = FitbitIntradayCollectorService(conn)
                results = service.collect_for_all_devices()

            total = sum(results.values())
            if total == 0:
                logger.warning("No devices found")
                time.sleep(NO_DEVICES_SLEEP_SECONDS)
                continue

            logger.info(
                f"Cycle: {results['success']} success, "
                f"{results['rate_limited']} rate-limited, {results['error']} errors"
            )

            if results["rate_limited"] == total and results["rate_limited"] > 0:
                logger.info("ALL devices rate-limited. Sleeping 10 minutes.")
                time.sleep(RATE_LIMIT_SLEEP_SECONDS)
            elif results["success"] == total:
                logger.info("All devices up to date. Sleeping 10 minutes.")
                time.sleep(UP_TO_DATE_SLEEP_SECONDS)
            else:
                time.sleep(CYCLE_SLEEP_SECONDS)

        except KeyboardInterrupt:
            logger.info("=== STOPPED BY USER ===")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
            time.sleep(NO_DEVICES_SLEEP_SECONDS)


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Working directory: {os.getcwd()}")

    try:
        main_loop()
    except KeyboardInterrupt:
        logger.info("\n=== STOPPED BY USER (Ctrl+C) ===")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
