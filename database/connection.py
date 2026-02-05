import psycopg2
from typing import Any, Optional, Union, List, Tuple
from config import DB_CONFIG


class ConnectionManager:
    """
    Manages PostgreSQL database connections and provides low-level query execution.
    
    This class handles connection lifecycle, transaction management, and basic
    query execution. Domain-specific logic should be implemented in repositories.
    """
    
    def __init__(self) -> None:
        """Initialize a ConnectionManager instance."""
        self.connection = None
        self.cursor = None

    def connect(self) -> bool:
        """
        Open a connection to the PostgreSQL database.

        Uses credentials from config.DB_CONFIG. On success,
        initializes a cursor for query execution.

        Returns:
            bool: True if connection succeeded, False otherwise.
        """
        try:
            self.connection = psycopg2.connect(
                host=DB_CONFIG["host"],
                database=DB_CONFIG["database"],
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"],
                port=DB_CONFIG["port"],
            )
            self.cursor = self.connection.cursor()
            return True
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            return False

    def close(self) -> None:
        """
        Close the open database cursor and connection.

        Ensures cleanup of resources. Safe to call even if
        connection was never established.
        """
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
        except Exception as e:
            print(f"Error closing the connection to the database: {e}")
        finally:
            self.cursor = None
            self.connection = None

    def commit(self) -> None:
        """
        Commit the current database transaction.

        No-op if there is no active connection. Should be
        called after INSERT/UPDATE/DELETE operations.
        """
        if self.connection:
            self.connection.commit()

    def rollback(self) -> None:
        """
        Roll back the current transaction.

        Useful to undo the last operation that raised an error.
        """
        if self.connection:
            self.connection.rollback()

    def execute_query(
        self, 
        query: str, 
        params: Optional[Tuple[Any, ...]] = None
    ) -> Union[List[Tuple[Any, ...]], bool, None]:
        """
        Execute any SQL query with optional parameters.

        This method handles execution, commits on success, and
        returns fetched results if present.

        Args:
            query (str): A SQL query to execute.
            params (tuple | list): Parameter values for parametric queries.

        Returns:
            list | bool | None: Fetched rows for SELECT,
                                 True for successful DDL/DML,
                                 None on failure.
        """
        try:
            self.cursor.execute(query, params or ())
            if self.cursor.description:  # If the query returns results
                result = self.cursor.fetchall()
                self.commit()
                return result
            self.commit()
            return True
        except Exception as e:
            print(f"Error executing query: {e}")
            self.rollback()
            return None

    def execute_many(
        self, 
        query: str, 
        params_list: List[Tuple[Any, ...]]
    ) -> bool:
        """
        Run the same query multiple times with batch parameters.

        Args:
            query (str): A SQL query with placeholders.
            params_list (list): A list of parameter tuples.

        Returns:
            bool: True if successful for all executions, False on any failure.
        """
        try:
            self.cursor.executemany(query, params_list)
            self.commit()
            return True
        except Exception as e:
            print(f"Error executing multiple queries: {e}")
            self.rollback()
            return False

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - handles cleanup."""
        if exc_type is not None:
            self.rollback()
        self.close()
