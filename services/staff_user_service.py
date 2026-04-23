from typing import Dict, Any, Optional, List

from database import StaffUserRepository, DeviceRepository
from database.orm_models import UserModel, StaffProfileModel, UserRole
from database.connection import SqlalchemyConnection
from services.result_enums import (
    ChangePasswordResult,
    AdminCreateStaffUserResult,
    AdminResetPasswordResult,
    AdminDeactivateStaffUserResult,
)

class StaffUserService:
    """
    Service for retrieving.
    """
    
    def __init__(self, connection_manager: SqlalchemyConnection):
        """
        Initialize the service with a connection manager.
        
        Args:
            connection_manager: Active SqlalchemyConnection instance
        """
        self.conn = connection_manager
        self.staff_user_repo = StaffUserRepository(connection_manager)
        self.device_repo = DeviceRepository(connection_manager)

    def get_role_for_user(self, user_id: int) -> Optional[str]:
        return self.staff_user_repo.get_role(user_id)

    def check_user(self, username: str, password: str):
        return self.staff_user_repo.verify_credentials(username, password)

    def get_user_info(self, user_id: int) -> Dict[str, Any]:
        user = self.staff_user_repo.get_by_id(user_id)
        profile = self.conn.session.query(StaffProfileModel).get(user_id)
        return {
            'id': user_id,
            'username': user.username,
            'full_name': user.full_name,
            'role': user.role.value,
            'created_at': user.created_at,
            'last_login': user.last_login,
            'care_provider_id': profile.care_provider_id if profile else None
        }

    def get_staff_users_by_care_provider(self, care_provider_id: int) -> List[Dict]:
        staff_users = self.staff_user_repo.get_by_care_provider(care_provider_id)
        return [{
            "id": u.id,
            "username": u.username,
            "full_name": u.full_name,
            "role": u.role.value,
            "created_at": u.created_at,
            "last_login": u.last_login,
            "is_active": u.is_active,
            "care_provider_id": care_provider_id,
        } for u in staff_users]


    def check_and_change_password(self, user_id: int, current_password: str, new_password: str) -> ChangePasswordResult:
        if self.staff_user_repo.verify_password(user_id, current_password):
            if self.staff_user_repo.update_password(user_id, new_password):
                return ChangePasswordResult.SUCCESS
            else:
                return ChangePasswordResult.ERROR
        else:
            return ChangePasswordResult.NO_CURRENT_PASSWORD


    def create_staff_user(self, username: str, full_name: str, password: str, care_provider_id: int) -> AdminCreateStaffUserResult:

        if self.staff_user_repo.username_exists(username.strip()):
            return AdminCreateStaffUserResult.USERNAME_EXISTS

        user_id = self.staff_user_repo.create(username.strip(), password, full_name.strip() or username.strip(), care_provider_id)
        if user_id:
            return AdminCreateStaffUserResult.SUCCESS
        return AdminCreateStaffUserResult.ERROR


    def admin_reset_staff_user_password(self, admin_user_id: int, target_user_id: int, new_password: str) -> AdminResetPasswordResult:
        if target_user_id == admin_user_id:
            return AdminResetPasswordResult.FORBIDDEN

        role = self.staff_user_repo.get_role(target_user_id)

        if role is None:
            return AdminResetPasswordResult.NOT_FOUND
        if role != UserRole.STAFF:
            return AdminResetPasswordResult.FORBIDDEN
        if self.staff_user_repo.update_password_for_staff_user(target_user_id, new_password):
            return AdminResetPasswordResult.SUCCESS
        return AdminResetPasswordResult.ERROR

    def admin_deactivate_staff_user(self, admin_user_id: int, target_user_id: int) -> AdminDeactivateStaffUserResult:
        if target_user_id == admin_user_id:
            return AdminDeactivateStaffUserResult.FORBIDDEN
        role = self.staff_user_repo.get_role(target_user_id)
        if role is None:
            return AdminDeactivateStaffUserResult.NOT_FOUND
        if role != UserRole.STAFF:
            return AdminDeactivateStaffUserResult.FORBIDDEN
        if self.staff_user_repo.deactivate_staff_user(target_user_id):
            return AdminDeactivateStaffUserResult.SUCCESS
        return AdminDeactivateStaffUserResult.ERROR
