from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Any, Optional

from database import ConnectionManager, AdminUserRepository, DeviceRepository, AdminUser
from services.result_enums import ChangePasswordResult


class AdminUserService:
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
        self.admin_repo = AdminUserRepository(connection_manager)
        self.device_repo = DeviceRepository(connection_manager)


    def check_user(self, username: str, password: str):
        return self.admin_repo.verify_credentials(username, password)

    def get_admin_user_info(self, admin_user_id: int) -> Dict[str, Any]:

        admin_user = self.admin_repo.get_by_id(admin_user_id)
        devices = self.device_repo.get_by_admin_user(admin_user_id)
            
        admin_user = {
                        'id': admin_user_id,
                        'username': admin_user.username,
                        'full_name': admin_user.full_name,
                        'created_at': admin_user.created_at,
                        'last_login': admin_user.last_login,
                        'num_devices': len(devices)
                    }

        return admin_user


    def check_and_change_password(self, admin_user_id: int, current_password: str, new_password: str) -> ChangePasswordResult:
        if self.admin_repo.verify_password(admin_user_id, current_password):
            if self.admin_repo.update_password(admin_user_id, new_password):
                return ChangePasswordResult.SUCCESS
            else:
                return ChangePasswordResult.ERROR
        else:
            return ChangePasswordResult.NO_CURRENT_PASSWORD
