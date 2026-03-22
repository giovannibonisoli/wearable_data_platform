from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Any, Optional

from database import ConnectionManager, CareProviderUserRepository, DeviceRepository, User
from services.result_enums import ChangePasswordResult


class CareProviderUserService:
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
        self.care_provider_repo = CareProviderUserRepository(connection_manager)
        self.device_repo = DeviceRepository(connection_manager)


    def check_user(self, username: str, password: str):
        return self.care_provider_repo.verify_credentials(username, password)

    def get_user_info(self, user_id: int) -> Dict[str, Any]:

        user = self.care_provider_repo.get_by_id(user_id)
        devices = self.device_repo.get_by_user(user_id)
            
        user = {
                        'id': user_id,
                        'username': user.username,
                        'full_name': user.full_name,
                        'created_at': user.created_at,
                        'last_login': user.last_login,
                        'num_devices': len(devices)
                    }

        return user


    def check_and_change_password(self, user_id: int, current_password: str, new_password: str) -> ChangePasswordResult:
        if self.care_provider_repo.verify_password(user_id, current_password):
            if self.care_provider_repo.update_password(user_id, new_password):
                return ChangePasswordResult.SUCCESS
            else:
                return ChangePasswordResult.ERROR
        else:
            return ChangePasswordResult.NO_CURRENT_PASSWORD
