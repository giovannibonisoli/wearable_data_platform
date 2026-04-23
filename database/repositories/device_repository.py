from typing import Optional, List, Tuple, Dict, Any
from datetime import datetime, date
from database.connection import SqlalchemyConnection
from database.orm_models import DeviceModel
from utils.encryption import encrypt_token, decrypt_token


class DeviceRepository:
    """
    Repository for device operations.
    
    Handles device management, OAuth tokens, and authorization status.
    """
    
    def __init__(self, connection_manager: SqlalchemyConnection):
        self.db = connection_manager

    def create(
        self,
        care_provider_id: int,
        email_address: str,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
    ) -> Optional[int]:
        device = DeviceModel(
            care_provider_id=care_provider_id,
            email_address=email_address,
            authorization_status='inserted',
            access_token=encrypt_token(access_token) if access_token else None,
            refresh_token=encrypt_token(refresh_token) if refresh_token else None,
        )
        self.db.session.add(device)
        self.db.session.flush()
        self.db.session.commit()
        return device.id

    def get_by_id(self, device_id: int) -> Optional[DeviceModel]:
        return self.db.session.query(DeviceModel).get(device_id)

    def get_by_email(self, email_address: str) -> Optional[DeviceModel]:
        return (
            self.db.session.query(DeviceModel)
            .filter(DeviceModel.email_address == email_address)
            .order_by(DeviceModel.created_at.desc())
            .first()
        )

    def get_by_care_provider(self, care_provider_id: int) -> List[DeviceModel]:
        return (
            self.db.session.query(DeviceModel)
            .filter(DeviceModel.care_provider_id == care_provider_id)
            .order_by(DeviceModel.created_at.desc())
            .all()
        )

    def get_all_authorized(self) -> List[DeviceModel]:
        return (
            self.db.session.query(DeviceModel)
            .filter(DeviceModel.authorization_status == 'authorized')
            .order_by(DeviceModel.created_at.desc())
            .all()
        )

    def get_all_authorized_by_care_provider(self, care_provider_id: int) -> List[DeviceModel]:
        return (
            self.db.session.query(DeviceModel)
            .filter(
                DeviceModel.care_provider_id == care_provider_id,
                DeviceModel.authorization_status == 'authorized'
            )
            .order_by(DeviceModel.created_at.desc())
            .all()
        )

    def update_status(self, device_id: int, auth_status: str) -> bool:
        assert auth_status in ['inserted', 'authorized', 'non_active'], \
            f"Invalid status: {auth_status}"
        device = self.db.session.query(DeviceModel).get(device_id)
        if device:
            device.authorization_status = auth_status
            self.db.session.commit()
            print(f"Status changed to {auth_status} for device {device_id}.")
            return True
        return False

    def update_device_type(self, device_id: int, device_type: str) -> bool:
        device = self.db.session.query(DeviceModel).get(device_id)
        if device:
            device.device_type = device_type
            self.db.session.commit()
            return True
        return False

    def get_tokens(self, device_id: int) -> Tuple[Optional[str], Optional[str]]:
        device = self.db.session.query(DeviceModel).get(device_id)
        if device and device.access_token and device.refresh_token:
            return decrypt_token(device.access_token), decrypt_token(device.refresh_token)
        return None, None

    def update_tokens(self, device_id: int, access_token: str, refresh_token: str) -> bool:
        device = self.db.session.query(DeviceModel).get(device_id)
        if device:
            device.access_token = encrypt_token(access_token)
            device.refresh_token = encrypt_token(refresh_token)
            self.db.session.commit()
            return True
        return False

    def update_last_synch(self, device_id: int, timestamp: datetime) -> bool:
        device = self.db.session.query(DeviceModel).get(device_id)
        if device:
            device.last_synch = timestamp
            self.db.session.commit()
            print(f"Last synch date {timestamp} for device_id {device_id} successfully updated.")
            return True
        return False

    def update_daily_summaries_checkpoint(self, device_id: int, date_value: date) -> bool:
        device = self.db.session.query(DeviceModel).get(device_id)
        if device:
            device.daily_summaries_checkpoint = date_value
            self.db.session.commit()
            print(f"Daily summaries checkpoint {date_value} for device_id {device_id} successfully updated.")
            return True
        return False

    def update_intraday_checkpoint(self, device_id: int, timestamp: datetime) -> bool:
        device = self.db.session.query(DeviceModel).get(device_id)
        if device:
            device.intraday_checkpoint = timestamp
            self.db.session.commit()
            print(f"Intraday checkpoint {timestamp} for device_id {device_id} successfully updated.")
            return True
        return False

    def update_sleep_checkpoint(self, device_id: int, date_value: date) -> bool:
        device = self.db.session.query(DeviceModel).get(device_id)
        if device:
            device.sleep_checkpoint = date_value
            self.db.session.commit()
            print(f"Sleep checkpoint {date_value} for device_id {device_id} successfully updated.")
            return True
        return False

    def get_last_synch(self, device_id: int) -> Optional[datetime]:
        device = self.db.session.query(DeviceModel).get(device_id)
        return device.last_synch if device else None

    def get_daily_summary_checkpoint(self, device_id: int) -> Optional[date]:
        device = self.db.session.query(DeviceModel).get(device_id)
        return device.daily_summaries_checkpoint if device else None

    def get_intraday_checkpoint(self, device_id: int) -> Optional[datetime]:
        device = self.db.session.query(DeviceModel).get(device_id)
        return device.intraday_checkpoint if device else None

    def get_sleep_checkpoint(self, device_id: int) -> Optional[date]:
        device = self.db.session.query(DeviceModel).get(device_id)
        return device.sleep_checkpoint if device else None

    def update_email_address(self, device_id: int, new_email: str) -> bool:
        device = self.db.session.query(DeviceModel).get(device_id)
        if device and device.authorization_status == 'inserted':
            device.email_address = new_email
            self.db.session.commit()
            return True
        return False