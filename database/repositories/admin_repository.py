import bcrypt
from typing import Optional, List, Dict, Any
from database.connection import ConnectionManager
from database.models import AdminUser


class AdminUserRepository:
    """
    Repository for admin user operations.
    
    Handles authentication, user management, and admin-specific queries.
    """
    
    def __init__(self, connection_manager: ConnectionManager):
        """
        Initialize the repository with a connection manager.
        
        Args:
            connection_manager: Active ConnectionManager instance
        """
        self.db = connection_manager

    def verify_credentials(self, username: str, password: str) -> Optional[AdminUser]:
        """
        Authenticate an admin user.

        Checks the username and bcrypt-hashed password against the admin_users table.
        On success, updates last_login to the current timestamp.

        Args:
            username: The admin username.
            password: The plaintext password to verify.

        Returns:
            AdminUser object on success, None if credentials are invalid or user inactive.
        """
        query = """
            SELECT id, username, password_hash, full_name
            FROM admin_users
            WHERE username = %s AND is_active = TRUE
        """
        result = self.db.execute_query(query, (username,))
        
        if result:
            user_id, username, password_hash, full_name = result[0]
            # Verify password
            if bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8')):
                # Update last login
                self.db.execute_query("""
                    UPDATE admin_users 
                    SET last_login = CURRENT_TIMESTAMP 
                    WHERE id = %s
                """, (user_id,))
                return {
                    'id': user_id,
                    'username': username,
                    'full_name': full_name
                }
        return None

    def get_by_id(self, admin_user_id: int) -> Optional[AdminUser]:
        """
        Fetch an admin user by ID.

        Args:
            admin_user_id: The ID of the admin user.

        Returns:
            AdminUser object or None if not found.
        """
        query = """
            SELECT id, username, full_name, created_at, last_login, is_active
            FROM admin_users
            WHERE id = %s
        """
        result = self.db.execute_query(query, (admin_user_id,))
        
        if result:
            row = result[0]
            return AdminUser(
                id=row[0],
                username=row[1],
                full_name=row[2],
                created_at=row[3],
                last_login=row[4],
                is_active=row[5]
            )
        return None

    def get_all(self) -> List[AdminUser]:
        """
        Retrieve all admin users.

        Returns:
            List of AdminUser objects ordered by creation date.
        """
        query = """
            SELECT id, username, email, full_name, created_at, last_login, is_active
            FROM admin_users
            ORDER BY created_at DESC
        """
        result = self.db.execute_query(query)
        
        if result:
            return [
                AdminUser(
                    id=row[0],
                    username=row[1],
                    email=row[2],
                    full_name=row[3],
                    created_at=row[4],
                    last_login=row[5],
                    is_active=row[6]
                )
                for row in result
            ]
        return []

    def update_password(self, admin_user_id: int, new_password: str) -> bool:
        """
        Change the stored password of an admin user.

        Args:
            admin_user_id: The user to update.
            new_password: New plaintext password.

        Returns:
            bool: True if update succeeded, False otherwise.
        """
        password_hash = bcrypt.hashpw(
            new_password.encode('utf-8'), 
            bcrypt.gensalt()
        ).decode('utf-8')
        
        query = """
            UPDATE admin_users
            SET password_hash = %s
            WHERE id = %s
        """
        result = self.db.execute_query(query, (password_hash, admin_user_id))
        return bool(result)

    def create(
        self, 
        username: str, 
        password: str, 
        full_name: str, 
        email: Optional[str] = None
    ) -> Optional[int]:
        """
        Create a new admin user.

        Args:
            username: Unique username
            password: Plaintext password (will be hashed)
            full_name: Full name of the admin
            email: Optional email address

        Returns:
            int: New user ID on success, None on failure
        """
        password_hash = bcrypt.hashpw(
            password.encode('utf-8'), 
            bcrypt.gensalt()
        ).decode('utf-8')
        
        query = """
            INSERT INTO admin_users (username, password_hash, full_name, email)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """
        result = self.db.execute_query(query, (username, password_hash, full_name, email))
        return result[0][0] if result else None

    def deactivate(self, admin_user_id: int) -> bool:
        """
        Deactivate an admin user (soft delete).

        Args:
            admin_user_id: The user to deactivate

        Returns:
            bool: True if successful
        """
        query = """
            UPDATE admin_users
            SET is_active = FALSE
            WHERE id = %s
        """
        result = self.db.execute_query(query, (admin_user_id,))
        return bool(result)

    def activate(self, admin_user_id: int) -> bool:
        """
        Reactivate a deactivated admin user.

        Args:
            admin_user_id: The user to activate

        Returns:
            bool: True if successful
        """
        query = """
            UPDATE admin_users
            SET is_active = TRUE
            WHERE id = %s
        """
        result = self.db.execute_query(query, (admin_user_id,))
        return bool(result)
