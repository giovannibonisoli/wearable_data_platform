from typing import Optional, List
from database.connection import ConnectionManager

from database.models import CareProvider


class CareProviderRepository:
    """
    Repository for care provider operations.
    
    Handles care provider management queries.
    """
    
    def __init__(self, connection_manager: ConnectionManager):
        """
        Initialize the repository with a connection manager.
        """

        self.db = connection_manager

    def get_by_id(self, care_provider_id: int) -> Optional[CareProvider]:
        """
        Fetch an care provider by ID.

        Args:
            care_provider_id: The ID of the care_provider_id.

        Returns:
            CareProvider object or None if not found.
        """
        query = """
            SELECT id, full_name, created_at
            FROM care_providers 
            id = %s
        """
        result = self.db.execute_query(query, (care_provider_id,))
        
        if result:
            row = result[0]
            return CareProvider(
                id=row[0],
                full_name=row[1],
                created_at=row[2]
            )
        return None

    
    def get_all(self) -> List[CareProvider]:
        """
        Retrieve all care providers.

        Returns:
            List of CareProvider objects ordered by creation date.
        """
        query = """
            SELECT id, full_name, created_at
            FROM care_providers
        """
        result = self.db.execute_query(query)
        
        if result:
            return [
                CareProvider(
                    id=row[0],
                    full_name=row[1],
                    created_at=row[2],
                )
                for row in result
            ]
        return []


    def create(self, full_name: str) -> Optional[int]:
        """
        Create a new care provider.

        Args:
            full_name: Full name of the care provider

        Returns:
            int: New care provider ID on success, None on failure
        """
        
        query = """
            INSERT INTO care_providers (full_name)
            VALUES (%s)
            RETURNING id
        """
        result = self.db.execute_query(
            query, (full_name,),
        )

        if not result:
            return None

        return result[0][0]


    def rename(self, care_provider_id: int, full_name: str)  -> bool:
        """
        Renam care provider 

        Args:
            care_provider_id: Care provider to rename
            full_name: new full name

        Returns:
            bool: True if successful
        """
        query = """
            UPDATE care_providers
            SET full_name = %s
            WHERE id = %s
        """
        result = self.db.execute_query(query, (full_name,care_provider_id))
        return bool(result)
