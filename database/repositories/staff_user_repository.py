import bcrypt
from typing import Optional, List
from database.connection import ConnectionManager
from database.models import StaffUser, USER_ROLE_ADMIN, USER_ROLE_STAFF


class StaffUserRepository:
    """
    Repository for staff user operations.
    
    Handles authentication, user management queries.
    """
    
    def __init__(self, connection_manager: ConnectionManager):
        """
        Initialize the repository with a connection manager.
        
        Args:
            connection_manager: Active ConnectionManager instance
        """
        self.db = connection_manager

    def verify_credentials(self, username: str, password: str) -> Optional[StaffUser]:
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
            SELECT id, username, password_hash, full_name, role
            FROM users
            WHERE username = %s AND is_active = TRUE
        """
        result = self.db.execute_query(query, (username,))
        
        if result:
            user_id, username, password_hash, full_name, role = result[0]
            if role not in (USER_ROLE_ADMIN, USER_ROLE_STAFF):
                return None
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
                    'full_name': full_name,
                    'role': role,
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
        

    def get_by_id(self, user_id: int) -> Optional[StaffUser]:
        """
        Fetch an user by ID.

        Args:
            users: The ID of the user.

        Returns:
            User object or None if not found.
        """
        query = """
            SELECT users.id, users.username, users.full_name, users.role, users.created_at, users.last_login, users.is_active, staff_profiles.care_provider_id
            FROM users 
            LEFT JOIN staff_profiles ON users.id = staff_profiles.user_id
            WHERE users.id = %s
        """
        result = self.db.execute_query(query, (user_id,))
        
        if result:
            row = result[0]
            return StaffUser(
                id=row[0],
                username=row[1],
                full_name=row[2],
                role=row[3],
                created_at=row[4],
                last_login=row[5],
                is_active=row[6],
                care_provider_id=row[7]
            )
        return None


    def get_by_care_provider(self, care_provider_id: int) -> List[StaffUser]:
        """
        Retrieve all staff users.

        Returns:
            List of User objects ordered by creation date.
        """
        query = """
            SELECT users.id, users.username, users.full_name, users.role, users.created_at, users.last_login, users.is_active, staff_profiles.care_provider_id
            FROM users
            LEFT JOIN staff_profiles ON users.id = staff_profiles.user_id
            WHERE care_provider_id = %s
        """
        result = self.db.execute_query(query, (care_provider_id, ))
        
        if result:
            return [
                StaffUser(
                    id=row[0],
                    username=row[1],
                    full_name=row[2],
                    role=row[3],
                    created_at=row[4],
                    last_login=row[5],
                    is_active=row[6],
                    care_provider_id=row[7]
                )
                for row in result
            ]
        return []

    def username_exists(self, username: str) -> bool:
        """Return True if a staff user with this username already exists."""
        query = "SELECT 1 FROM users WHERE username = %s"
        result = self.db.execute_query(query, (username,))
        return bool(result)

    def get_role(self, user_id: int) -> Optional[str]:
        """Return the role for a user, or None if missing."""
        query = "SELECT role FROM users WHERE id = %s"
        result = self.db.execute_query(query, (user_id,))
        if result:
            return result[0][0]
        return None

    # def list_care_providers_with_device_counts(self) -> List[Dict[str, Any]]:
    #     """
    #     All care_provider users with device counts (including inactive users).
    #     """
    #     query = """
    #         SELECT u.id, u.username, u.full_name, u.is_active,
    #                COUNT(d.id)::int AS device_count
    #         FROM users u
    #         LEFT JOIN devices d ON d.user_id = u.id
    #         WHERE u.role = %s
    #         GROUP BY u.id, u.username, u.full_name, u.is_active
    #         ORDER BY u.username
    #     """
    #     result = self.db.execute_query(query, (USER_ROLE_STAFF,))
    #     if not result:
    #         return []
    #     return [
    #         {
    #             "id": row[0],
    #             "username": row[1],
    #             "full_name": row[2] or "",
    #             "is_active": row[3],
    #             "device_count": row[4],
    #         }
    #         for row in result
    #     ]

    def update_password_for_staff_user(self, user_id: int, new_password: str) -> bool:
        """Set password for a staff_user user (admin reset). Returns False if not a staff_user."""
        password_hash = bcrypt.hashpw(
            new_password.encode('utf-8'),
            bcrypt.gensalt()
        ).decode('utf-8')
        query = """
            UPDATE users
            SET password_hash = %s
            WHERE id = %s AND role = %s
            RETURNING id
        """
        result = self.db.execute_query(query, (password_hash, user_id, USER_ROLE_STAFF))
        return bool(result)

    def deactivate_staff_user(self, user_id: int) -> bool:
        """Soft-deactivate a user only if they are a staff_user."""
        query = """
            UPDATE users
            SET is_active = FALSE
            WHERE id = %s AND role = %s
            RETURNING id
        """
        result = self.db.execute_query(query, (user_id, USER_ROLE_STAFF))
        return bool(result)

    def update_password(self, user_id: int, new_password: str) -> bool:
        """
        Change the stored password of an staff user.

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

    def create(self, username: str, password: str, full_name: str, care_provider_id: int) -> Optional[int]:
        """
        Create a new user.

        Args:
            username: Unique username
            password: Plaintext password (will be hashed)
            full_name: Full name of the user
            care_provider_id: ID of the care provider

        Returns:
            int: New user ID on success, None on failure
        """
        password_hash = bcrypt.hashpw(
            password.encode('utf-8'), 
            bcrypt.gensalt()
        ).decode('utf-8')
        
        query = """
            INSERT INTO users (username, password_hash, full_name, role)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """
        result = self.db.execute_query(
            query,
            (username, password_hash, full_name, USER_ROLE_STAFF),
        )

        if not result:
            return None

        query = """
            INSERT INTO staff_profiles(user_id, care_provider_id)
            VALUES (%s, %s)
        """
        result_staff_profile = self.db.execute_query(query, (result[0][0], care_provider_id))

        if not result_staff_profile:
            return None

        return result[0][0]
        

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
