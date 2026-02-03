# Mock Database for Testing

This directory contains a mock database implementation that allows you to run tests without requiring a real PostgreSQL database connection.

## Overview

The `MockDatabaseManager` class mimics the behavior of the real `DatabaseManager` but stores all data in memory. This provides several benefits:

- ✅ **No database setup required** - Tests run without PostgreSQL
- ✅ **Faster test execution** - No network calls or database I/O
- ✅ **Isolated tests** - Each test can have a fresh database state
- ✅ **Easy debugging** - Inspect data structures directly
- ✅ **CI/CD friendly** - No database dependencies in test environments

## Files

- `mock_db.py` - The `MockDatabaseManager` class implementation
- `test_helpers.py` - Helper functions and decorators for using mock database
- `example_mock_test.py` - Examples showing different usage patterns

## Quick Start

### Method 1: Direct Usage (Simplest)

```python
from tests.mock_db import MockDatabaseManager

def test_my_function():
    db = MockDatabaseManager()
    db.connect()
    
    # Use it just like DatabaseManager
    user_id = db.add_user("Test User", "test@example.com")
    db.insert_daily_summary(user_id, datetime.now().date(), steps=10000)
    
    summaries = db.get_daily_summaries(user_id)
    assert len(summaries) == 1
    
    db.close()
```

### Method 2: Using the Decorator

```python
from tests.test_helpers import use_mock_database

@use_mock_database
def test_my_function():
    from db import DatabaseManager
    
    # DatabaseManager is automatically replaced with MockDatabaseManager
    db = DatabaseManager()
    db.connect()
    
    user_id = db.add_user("Test User", "test@example.com")
    # ... rest of your test
```

### Method 3: Using Context Manager

```python
from tests.test_helpers import MockDatabaseContext

def test_my_function():
    with MockDatabaseContext():
        from db import DatabaseManager
        
        db = DatabaseManager()  # This is MockDatabaseManager
        db.connect()
        
        # ... your test code
```

## Supported Methods

The `MockDatabaseManager` implements all the key methods from `DatabaseManager`:

### User/Email Management
- `add_user(name, email)` - Add a test user
- `add_email_address(admin_user_id, address_name, ...)` - Add email address
- `get_email_id_by_name(address_name)` - Get email ID by name

### Daily Summaries
- `insert_daily_summary(email_id, date, **data)` - Insert/update daily summary
- `get_daily_summaries(email_id, start_date=None, end_date=None)` - Get summaries
- `get_daily_summary_checkpoint(email_id)` - Get checkpoint date

### Intraday Metrics
- `insert_intraday_metric(email_id, timestamp, data_type, value)` - Insert metric
- `get_intraday_metrics(email_id, metric_type, start_time=None, end_time=None)` - Get metrics
- `check_intraday_timestamp(email_id, timestamp)` - Check if timestamp exists
- `get_intraday_checkpoint(email_id)` - Get checkpoint timestamp

### Sleep Logs
- `insert_sleep_log(email_id, start_time, end_time, **data)` - Insert sleep log
- `get_sleep_logs(email_id, start_date=None, end_date=None)` - Get sleep logs

### Alerts
- `insert_alert(email_id, alert_type, priority, triggering_value, threshold, ...)` - Insert alert
- `get_user_alerts(email_id, start_time=None, end_time=None, acknowledged=None)` - Get alerts

### Utility Methods
- `reset()` - Clear all data (useful for test cleanup)
- `get_all_data()` - Get all data for inspection/debugging

## Example: Adapting Existing Tests

### Before (using real database):

```python
from db import DatabaseManager, reset_database, init_db

def test_data_insertion():
    reset_database()
    init_db()
    
    db = DatabaseManager()
    db.connect()
    user_id = db.add_user("Test User", "test@example.com")
    # ... rest of test
```

### After (using mock database):

```python
from tests.mock_db import MockDatabaseManager

def test_data_insertion():
    db = MockDatabaseManager()
    db.connect()
    
    user_id = db.add_user("Test User", "test@example.com")
    # ... rest of test (same code!)
    
    # Optional: reset for cleanup
    db.reset()
```

Or using the decorator:

```python
from tests.test_helpers import use_mock_database

@use_mock_database
def test_data_insertion():
    from db import DatabaseManager
    
    db = DatabaseManager()  # Automatically uses MockDatabaseManager
    db.connect()
    user_id = db.add_user("Test User", "test@example.com")
    # ... rest of test
```

## Testing Tips

### 1. Reset Between Tests

```python
def test_something():
    db = MockDatabaseManager()
    db.connect()
    
    try:
        # Your test code
        pass
    finally:
        db.reset()  # Clean up
        db.close()
```

### 2. Inspect Data for Debugging

```python
db = MockDatabaseManager()
db.connect()
# ... insert some data

all_data = db.get_all_data()
print(f"Daily summaries: {len(all_data['daily_summaries'])}")
print(f"Alerts: {len(all_data['alerts'])}")
```

### 3. Test Edge Cases

```python
# Test with no data
db = MockDatabaseManager()
db.connect()
summaries = db.get_daily_summaries(999)  # Non-existent user
assert summaries == []

# Test with date ranges
summaries = db.get_daily_summaries(
    user_id, 
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2025, 1, 31)
)
```

## Limitations

⚠️ **Note**: The mock database is designed for unit testing. It does not:

- Execute actual SQL queries (use `execute_query` with caution)
- Enforce database constraints (foreign keys, unique constraints, etc.)
- Support TimeScaleDB-specific features
- Handle complex SQL operations

For integration tests that require real database behavior, use a test database instance.

## Running the Examples

To see the mock database in action:

```bash
python tests/example_mock_test.py
```

## Migration Guide

To migrate existing tests to use the mock database:

1. **Option A**: Replace `DatabaseManager()` with `MockDatabaseManager()` directly
2. **Option B**: Use the `@use_mock_database` decorator to automatically patch imports
3. **Option C**: Use `MockDatabaseContext()` context manager for temporary mocking

Choose the method that best fits your test structure and preferences.
