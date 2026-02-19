
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Any, Optional

from database import ConnectionManager, DeviceRepository, AuthorizationRepository, Device 

class DeviceService:
    """
    Service for retrieving.
    
    This service encapsulates business logic for handling device authorization and get basic
    info.
    """
    
    def __init__(self, connection_manager: ConnectionManager):
        """
        Initialize the service with a connection manager.
        
        Args:
            connection_manager: Active ConnectionManager instance
        """
        self.conn = connection_manager
        self.auth_repo = AuthorizationRepository(connection_manager)
        self.device_repo = DeviceRepository(connection_manager)

    
    def get_devices_info_by_admin_user(self, admin_user_id: int) -> list[dict]:

        devices = self.device_repo.get_by_admin_user(admin_user_id)
        
        devices_data = []
        for device in devices:
            auth_status = device.authorization_status

            devices_data.append({
                    "id": device.id,
                    "email_address": device.email_address,
                    "device_type": device.device_type if device.device_type else "",
                    "auth_status": auth_status,
                    "is_pending_auth": self.auth_repo.check_exists(device.id)
                })

        return devices_data


    def add_new_device(self, admin_user_id: int, email_address: str) -> int:
        
        existing = self.device_repo.get_by_email(email_address)

        if existing:
            return "already_exists"
            
        # Create new device
        device_id = self.device_repo.create(
            admin_user_id=admin_user_id,
            email_address=email_address
        )
            
        if device_id:
            return "added"
        else:
            return "error"




    
        