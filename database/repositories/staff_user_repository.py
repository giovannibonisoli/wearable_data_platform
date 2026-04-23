import bcrypt
from datetime import datetime
from typing import Optional, List
from database.connection import SqlalchemyConnection
from database.orm_models import UserModel, StaffProfileModel, UserRole


class StaffUserRepository:
    def __init__(self, connection_manager: SqlalchemyConnection):
        self.db = connection_manager

    def verify_credentials(self, username: str, password: str) -> Optional[dict]:
        user = (
            self.db.session.query(UserModel)
            .filter(
                UserModel.username == username,
                UserModel.is_active == True
            )
            .first()
        )
        if not user or user.role not in (UserRole.ADMIN, UserRole.STAFF):
            return None
        if bcrypt.checkpw(password.encode('utf-8'), user.password_hash.encode('utf-8')):
            user.last_login = datetime.utcnow()
            self.db.session.commit()
            return {
                'id': user.id,
                'username': user.username,
                'full_name': user.full_name,
                'role': user.role.value,
            }
        return None

    def verify_password(self, user_id: int, password: str) -> bool:
        user = (
            self.db.session.query(UserModel)
            .filter(UserModel.id == user_id, UserModel.is_active == True)
            .first()
        )
        if user and bcrypt.checkpw(password.encode('utf-8'), user.password_hash.encode('utf-8')):
            return True
        return False

    def get_by_id(self, user_id: int) -> Optional[UserModel]:
        return self.db.session.query(UserModel).get(user_id)

    def get_by_care_provider(self, care_provider_id: int) -> List[UserModel]:
        return (
            self.db.session.query(UserModel)
            .join(StaffProfileModel, UserModel.id == StaffProfileModel.user_id)
            .filter(StaffProfileModel.care_provider_id == care_provider_id)
            .all()
        )

    def username_exists(self, username: str) -> bool:
        return self.db.session.query(UserModel).filter(UserModel.username == username).first() is not None

    def get_role(self, user_id: int) -> Optional[str]:
        user = self.db.session.query(UserModel).get(user_id)
        return user.role.value if user else None

    def update_password_for_staff_user(self, user_id: int, new_password: str) -> bool:
        user = (
            self.db.session.query(UserModel)
            .filter(UserModel.id == user_id, UserModel.role == UserRole.STAFF)
            .first()
        )
        if user:
            user.password_hash = bcrypt.hashpw(
                new_password.encode('utf-8'), bcrypt.gensalt()
            ).decode('utf-8')
            self.db.session.commit()
            return True
        return False

    def deactivate_staff_user(self, user_id: int) -> bool:
        user = (
            self.db.session.query(UserModel)
            .filter(UserModel.id == user_id, UserModel.role == UserRole.STAFF)
            .first()
        )
        if user:
            user.is_active = False
            self.db.session.commit()
            return True
        return False

    def update_password(self, user_id: int, new_password: str) -> bool:
        user = self.db.session.query(UserModel).get(user_id)
        if user:
            user.password_hash = bcrypt.hashpw(
                new_password.encode('utf-8'), bcrypt.gensalt()
            ).decode('utf-8')
            self.db.session.commit()
            return True
        return False

    def create(self, username: str, password: str, full_name: str, care_provider_id: int) -> Optional[int]:
        password_hash = bcrypt.hashpw(
            password.encode('utf-8'), bcrypt.gensalt()
        ).decode('utf-8')
        
        user = UserModel(
            username=username,
            password_hash=password_hash,
            full_name=full_name,
            role=UserRole.STAFF,
        )
        self.db.session.add(user)
        self.db.session.flush()

        profile = StaffProfileModel(user_id=user.id, care_provider_id=care_provider_id)
        self.db.session.add(profile)
        self.db.session.commit()
        return user.id

    def deactivate(self, user_id: int) -> bool:
        user = self.db.session.query(UserModel).get(user_id)
        if user:
            user.is_active = False
            self.db.session.commit()
            return True
        return False

    def activate(self, user_id: int) -> bool:
        user = self.db.session.query(UserModel).get(user_id)
        if user:
            user.is_active = True
            self.db.session.commit()
            return True
        return False