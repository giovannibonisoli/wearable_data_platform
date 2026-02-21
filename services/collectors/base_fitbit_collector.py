"""
Base class for Fitbit data collectors.

Provides common logic: device iteration, result aggregation.
Subclasses implement _process_one_device() with collection-specific logic.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict

from database import ConnectionManager, DeviceRepository, Device
from services.result_enums import CollectorResult

logger = logging.getLogger(__name__)


class BaseFitbitCollector(ABC):
    """
    Base collector for Fitbit data. Subclasses implement _process_one_device().
    """

    def __init__(self, conn: ConnectionManager):
        self.conn = conn
        self.device_repo = DeviceRepository(conn)

    @abstractmethod
    def _process_one_device(self, device: Device) -> str:
        """
        Process one device. Must return 'success', 'rate_limited', or 'error'.
        """
        raise NotImplementedError

    def collect_for_device(self, device_id: int) -> str:
        """
        Collect data for a single device by ID.

        Returns:
            'success', 'rate_limited', or 'error'
        """
        device = self.device_repo.get_by_id(device_id)
        if not device:
            logger.warning(f"Device {device_id} not found")
            return CollectorResult.ERROR.value
        if device.authorization_status != "authorized":
            logger.warning(f"Device {device_id} is not authorized")
            return CollectorResult.ERROR.value
        return self._process_one_device(device)

    def collect_for_all_devices(self) -> Dict[str, int]:
        """
        Collect data for all authorized devices.

        Returns:
            Dict with keys 'success', 'rate_limited', 'error' and counts.
        """
        devices = self.device_repo.get_all_authorized()
        if not devices:
            logger.warning("No authorized devices found")
            return {"success": 0, "rate_limited": 0, "error": 0}

        results: Dict[str, int] = {"success": 0, "rate_limited": 0, "error": 0}
        for device in devices:
            result = self._process_one_device(device)
            results[result] = results.get(result, 0) + 1

        return results
