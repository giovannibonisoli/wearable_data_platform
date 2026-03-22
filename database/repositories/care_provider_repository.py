import bcrypt
from typing import Optional, List, Dict, Any
from database.connection import ConnectionManager
from database.models import User


class CareProviderUserRepository:
    """
    Repository for user operations.
    
    Handles authentication, user management queries.
    """
    
    def __init__(self, connection_manager: ConnectionManager):
        """
        Initialize the repository with a connection manager.
        
        Args:
            connection_manager: Active ConnectionManager instance
        """
        self.db = connection_manager

    def verify_credentials(self, username: str, password: str) -> Optional[User]:
        """
        Authenticate an user.

        Checks the username and bcrypt-hashed password against the users table.
        On success, updates last_login to the current timestamp.

        Args:
            username: The username.
            password: The plaintext password to verify.

        Returns:
            User object on success, None if credentials are invalid or user inactive.
        """
        query = """
            SELECT id, username, password_hash, full_name
            FROM users
            WHERE username = %s AND is_active = TRUE
        """
        result = self.db.execute_query(query, (username,))
        
        if result:
            user_id, username, password_hash, full_name = result[0]
            # Verify password
            if bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8')):
                # Update last login
                self.db.execute_query("""
                    UPDATE users 
                    SET last_login = CURRENT_TIMESTAMP 
                    WHERE id = %s
                """, (user_id,))
                return {
                    'id': user_id,
                    'username': username,
                    'full_name': full_name
                }
        return None


    def verify_password(self, user_id: int, password: str) -> bool:

        query = """
            SELECT password_hash
            FROM users
            WHERE id = %s AND is_active = TRUE
        """

        result = self.db.execute_query(query, (user_id,))

        if result:
            password_hash = result[0][0]

            if bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8')):
                return True

        return False
        

    def get_by_id(self, user_id: int) -> Optional[User]:
        """
        Fetch an user by ID.

        Args:
            users: The ID of the user.

        Returns:
            User object or None if not found.
        """
        query = """
            SELECT id, username, full_name, created_at, last_login, is_active
            FROM users
            WHERE id = %s
        """
        result = self.db.execute_query(query, (user_id,))
        
        if result:
            row = result[0]
            return User(
                id=row[0],
                username=row[1],
                full_name=row[2],
                created_at=row[3],
                last_login=row[4],
                is_active=row[5]
            )
        return None

    def get_all(self) -> List[User]:
        """
        Retrieve all users.

        Returns:
            List of User objects ordered by creation date.
        """
        query = """
            SELECT id, username, email, full_name, created_at, last_login, is_active
            FROM users
            ORDER BY created_at DESC
        """
        result = self.db.execute_query(query)
        
        if result:
            return [
                User(
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

    def update_password(self, user_id: int, new_password: str) -> bool:
        """
        Change the stored password of an user.

        Args:
            user_id: The user to update.
            new_password: New plaintext password.

        Returns:
            bool: True if update succeeded, False otherwise.
        """
        password_hash = bcrypt.hashpw(
            new_password.encode('utf-8'), 
            bcrypt.gensalt()
        ).decode('utf-8')
        
        query = """
            UPDATE users
            SET password_hash = %s
            WHERE id = %s
        """
        result = self.db.execute_query(query, (password_hash, user_id))
        return bool(result)

    def create(
        self, 
        username: str, 
        password: str, 
        full_name: str, 
        email: Optional[str] = None
    ) -> Optional[int]:
        """
        Create a new user.

        Args:
            username: Unique username
            password: Plaintext password (will be hashed)
            full_name: Full name of the user
            email: Optional email address

        Returns:
            int: New user ID on success, None on failure
        """
        password_hash = bcrypt.hashpw(
            password.encode('utf-8'), 
            bcrypt.gensalt()
        ).decode('utf-8')
        
        query = """
            INSERT INTO users (username, password_hash, full_name, email)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """
        result = self.db.execute_query(query, (username, password_hash, full_name, email))
        return result[0][0] if result else None

    def deactivate(self, user_id: int) -> bool:
        """
        Deactivate an user (soft delete).

        Args:
            user_id: The user to deactivate

        Returns:
            bool: True if successful
        """
        query = """
            UPDATE users
            SET is_active = FALSE
            WHERE id = %s
        """
        result = self.db.execute_query(query, (user_id,))
        return bool(result)

    def activate(self, user_id: int) -> bool:
        """
        Reactivate a deactivated user.

        Args:
            user_id: The user to activate

        Returns:
            bool: True if successful
        """
        query = """
            UPDATE users
            SET is_active = TRUE
            WHERE id = %s
        """
        result = self.db.execute_query(query, (user_id,))
        return bool(result)
