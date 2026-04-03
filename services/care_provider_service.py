from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Any

from database import ConnectionManager, CareProviderRepository, DeviceRepository, StaffUserRepository

from services.result_enums import (
    AdminCreateCareProviderResult
)


class CareProviderService:
    """
    Service for handling care providers operationd.

    """
    
    def __init__(self, connection_manager: ConnectionManager):
        """
        Initialize the service with a connection manager.
        
        Args:
            connection_manager: Active ConnectionManager instance
        """
        self.conn = connection_manager
        self.care_provider_repo = CareProviderRepository(connection_manager)
        self.device_repo = DeviceRepository(connection_manager)
        self.staff_user_repo = StaffUserRepository(connection_manager)

    
    def get_all_care_providers(self) -> List[Dict]:

        care_providers = self.care_provider_repo.get_all()

        care_providers_data = []
        for care_provider in care_providers:

            device_count = len(self.device_repo.get_by_care_provider(care_provider.id))
            staff_users_count = len(self.staff_user_repo.get_by_care_provider(care_provider.id))
            
            care_providers_data.append({
                "id": care_provider.id,
                "full_name": care_provider.full_name,
                "created_at": care_provider.created_at,
                "staff_users_count": staff_users_count,
                "device_count": device_count
            })

        return care_providers_data

    
    def create_care_provider(self, full_name: str) -> List[Dict]:

        care_provider_id = self.care_provider_repo.create(full_name.strip())
        if care_provider_id:
            return AdminCreateCareProviderResult.SUCCESS
        return AdminCreateCareProviderResult.ERROR

