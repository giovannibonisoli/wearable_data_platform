from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Any, Optional

from database import ConnectionManager, CareProviderUserRepository, DeviceRepository, User
from database.models import USER_ROLE_CARE_PROVIDER
from services.result_enums import (
    ChangePasswordResult,
    AdminCreateCareProviderResult,
    AdminResetPasswordResult,
    AdminDeactivateCareProviderResult,
)


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

    def get_role_for_user(self, user_id: int) -> Optional[str]:
        return self.care_provider_repo.get_role(user_id)

    def check_user(self, username: str, password: str):
        return self.care_provider_repo.verify_credentials(username, password)

    def get_user_info(self, user_id: int) -> Dict[str, Any]:

        user = self.care_provider_repo.get_by_id(user_id)
        devices = self.device_repo.get_by_user(user_id)
            
        user = {
                        'id': user_id,
                        'username': user.username,
                        'full_name': user.full_name,
                        'role': user.role,
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

    def list_care_providers_for_admin(self) -> List[Dict[str, Any]]:
        return self.care_provider_repo.list_care_providers_with_device_counts()

    def admin_create_care_provider(
        self, username: str, full_name: str, password: str
    ) -> AdminCreateCareProviderResult:
        if self.care_provider_repo.username_exists(username.strip()):
            return AdminCreateCareProviderResult.USERNAME_EXISTS
        user_id = self.care_provider_repo.create(
            username.strip(), password, full_name.strip() or username.strip(), email=None
        )
        if user_id:
            return AdminCreateCareProviderResult.SUCCESS
        return AdminCreateCareProviderResult.ERROR

    def admin_reset_care_provider_password(
        self, admin_user_id: int, target_user_id: int, new_password: str
    ) -> AdminResetPasswordResult:
        if target_user_id == admin_user_id:
            return AdminResetPasswordResult.FORBIDDEN
        role = self.care_provider_repo.get_role(target_user_id)
        if role is None:
            return AdminResetPasswordResult.NOT_FOUND
        if role != USER_ROLE_CARE_PROVIDER:
            return AdminResetPasswordResult.FORBIDDEN
        if self.care_provider_repo.update_password_for_care_provider(target_user_id, new_password):
            return AdminResetPasswordResult.SUCCESS
        return AdminResetPasswordResult.ERROR

    def admin_deactivate_care_provider(
        self, admin_user_id: int, target_user_id: int
    ) -> AdminDeactivateCareProviderResult:
        if target_user_id == admin_user_id:
            return AdminDeactivateCareProviderResult.FORBIDDEN
        role = self.care_provider_repo.get_role(target_user_id)
        if role is None:
            return AdminDeactivateCareProviderResult.NOT_FOUND
        if role != USER_ROLE_CARE_PROVIDER:
            return AdminDeactivateCareProviderResult.FORBIDDEN
        if self.care_provider_repo.deactivate_care_provider(target_user_id):
            return AdminDeactivateCareProviderResult.SUCCESS
        return AdminDeactivateCareProviderResult.ERROR
