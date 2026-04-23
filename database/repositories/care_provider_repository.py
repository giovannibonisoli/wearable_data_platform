from datetime import datetime
from typing import Optional, List
from database.connection import SqlalchemyConnection
from database.orm_models import CareProviderModel


class CareProviderRepository:
    def __init__(self, connection_manager: SqlalchemyConnection):
        self.db = connection_manager

    def get_by_id(self, care_provider_id: int) -> Optional[CareProviderModel]:
        return self.db.session.query(CareProviderModel).get(care_provider_id)

    def get_all(self) -> List[CareProviderModel]:
        return self.db.session.query(CareProviderModel).all()

    def create(self, full_name: str) -> Optional[int]:
        care_provider = CareProviderModel(full_name=full_name, created_at=datetime.now())
        self.db.session.add(care_provider)
        self.db.session.commit()
        return care_provider.id

    def rename(self, care_provider_id: int, full_name: str) -> bool:
        care_provider = self.db.session.query(CareProviderModel).get(care_provider_id)
        if care_provider:
            care_provider.full_name = full_name
            self.db.session.commit()
            return True
        return False