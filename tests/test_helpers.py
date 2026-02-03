"""
Test helpers for using mock database in tests.

This module provides utilities to easily switch between real and mock databases
in your tests.
"""

import os
import sys
from unittest.mock import patch

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.mock_db import MockDatabaseManager


def use_mock_database(test_func):
    """
    Decorator to use mock database for a test function.
    
    Usage:
        @use_mock_database
        def test_something():
            from db import DatabaseManager
            db = DatabaseManager()  # Will use MockDatabaseManager
            ...
    """
    def wrapper(*args, **kwargs):
        with patch('db.DatabaseManager', MockDatabaseManager):
            return test_func(*args, **kwargs)
    wrapper.__name__ = test_func.__name__
    return wrapper


class MockDatabaseContext:
    """
    Context manager to temporarily use mock database.
    
    Usage:
        with MockDatabaseContext():
            from db import DatabaseManager
            db = DatabaseManager()  # Will use MockDatabaseManager
            ...
    """
    
    def __init__(self):
        self.patcher = None
    
    def __enter__(self):
        self.patcher = patch('db.DatabaseManager', MockDatabaseManager)
        self.patcher.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.patcher:
            self.patcher.stop()
        return False


def get_mock_db():
    """
    Get a new instance of MockDatabaseManager.
    
    Usage:
        mock_db = get_mock_db()
        mock_db.connect()
        user_id = mock_db.add_user("Test User", "test@example.com")
        ...
    """
    return MockDatabaseManager()
