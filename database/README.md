# Database Refactoring - Repository Pattern

This refactoring transforms the monolithic `DatabaseManager` class into a clean, maintainable repository pattern with domain-specific repositories.

## 📁 New Structure

```
database/
├── __init__.py                      # Package exports
├── connection.py                    # ConnectionManager (connection handling)
├── facade.py                        # Database (backward-compatible interface)
├── models.py                        # Data models (dataclasses)
└── repositories/
    ├── admin_repository.py          # Admin user operations
    ├── device_repository.py         # Device management
    ├── metrics_repository.py        # Health metrics
    ├── sleep_repository.py          # Sleep data
    ├── alert_repository.py          # Alerts
    └── authorization_repository.py  # OAuth pending auth
```

## 🎯 Key Improvements

### 1. **Separation of Concerns**
Each repository handles a single domain:
- `AdminUserRepository` → Admin authentication & management
- `DeviceRepository` → Device lifecycle & tokens
- `MetricsRepository` → Daily summaries & intraday data
- `SleepRepository` → Sleep sessions & logs
- `AlertRepository` → Health alerts
- `AuthorizationRepository` → OAuth flow

### 2. **Type Safety with Data Models**
Instead of raw tuples, we now use dataclasses:

```python
# Before
result = db.get_admin_user_by_id(1)
username = result[0]  # What's at index 0?

# After
user = admin_repo.get_by_id(1)
username = user.username  # Clear and autocomplete-friendly
```

### 3. **Consistent Return Types**
- Methods return domain objects (not tuples)
- Boolean returns for success/failure
- `None` for "not found"
- Lists for collections (never `None`)

### 4. **Better Testability**
Each repository can be tested independently with mocked connections.

## 🚀 Usage Examples

### Option 1: Using Repositories Directly (Recommended for new code)

```python
from database import ConnectionManager, AdminUserRepository, DeviceRepository

# Context manager handles connection lifecycle
with ConnectionManager() as db:
    admin_repo = AdminUserRepository(db)
    device_repo = DeviceRepository(db)
    
    # Authenticate admin
    user = admin_repo.verify_credentials("admin", "password123")
    if user:
        print(f"Welcome, {user.full_name}!")
        
        # Get their devices
        devices = device_repo.get_by_admin_user(user.id)
        for device in devices:
            print(f"Device: {device.email_address} - {device.authorization_status}")
```

### Option 2: Using the Facade (Backward compatible)

```python
from database import Database

db = Database()
if db.connect():
    # Old interface still works
    user = db.verify_admin_user("admin", "password123")
    if user:
        devices = db.get_admin_user_devices(user['id'])
    
    db.close()

# Or with context manager
with Database() as db:
    user = db.verify_admin_user("admin", "password123")
```

### Working with Metrics

```python
from database import ConnectionManager, MetricsRepository
from datetime import date, datetime, timedelta

with ConnectionManager() as db:
    metrics_repo = MetricsRepository(db)
    
    # Get daily summaries for last 7 days
    end_date = date.today()
    start_date = end_date - timedelta(days=7)
    
    summaries = metrics_repo.get_daily_summaries(
        device_id=42,
        start_date=start_date,
        end_date=end_date
    )
    
    for summary in summaries:
        print(f"{summary.date}: {summary.steps} steps, {summary.calories} cal")
    
    # Insert new daily summary
    metrics_repo.insert_daily_summary(
        device_id=42,
        date_value=date.today(),
        steps=10000,
        heart_rate=72.5,
        calories=2500.0
    )
```

### Working with Sleep Data

```python
from database import ConnectionManager, SleepRepository

with ConnectionManager() as db:
    sleep_repo = SleepRepository(db)
    
    # Get recent sleep logs
    sleep_logs = sleep_repo.get_sleep_logs(
        device_id=42,
        start_date=datetime.now() - timedelta(days=7)
    )
    
    for log in sleep_logs:
        print(f"Sleep: {log.start_time} - {log.end_time}")
        print(f"  Duration: {log.duration}s, Asleep: {log.minutes_asleep}m")
    
    # Insert complete sleep data from API
    sleep_data = {
        'startTime': datetime(2024, 1, 1, 22, 0),
        'endTime': datetime(2024, 1, 2, 6, 30),
        'isMainSleep': True,
        'duration': 30600000,  # milliseconds
        'minutesAsleep': 450,
        'minutesAwake': 60,
        'timeInBed': 510,
        'logType': 'auto_detected',
        'type': 'stages',
        'levels': {
            'data': [
                {'dateTime': datetime(2024, 1, 1, 22, 0), 'level': 'light', 'seconds': 3600},
                {'dateTime': datetime(2024, 1, 1, 23, 0), 'level': 'deep', 'seconds': 5400}
            ],
            'shortData': [
                {'dateTime': datetime(2024, 1, 1, 22, 30), 'seconds': 180}
            ]
        }
    }
    
    session_id = sleep_repo.insert_complete_sleep_data(device_id=42, sleep_data=sleep_data)
```

### Managing Alerts

```python
from database import ConnectionManager, AlertRepository

with ConnectionManager() as db:
    alert_repo = AlertRepository(db)
    
    # Create a high-priority alert
    alert_id = alert_repo.create(
        email_id=1,
        alert_type="heart_rate_high",
        priority="high",
        triggering_value=180.0,
        threshold=160.0,
        details="Heart rate exceeded safe threshold during exercise"
    )
    
    # Get unacknowledged alerts
    alerts = alert_repo.get_alerts(email_id=1, acknowledged=False)
    print(f"You have {len(alerts)} unacknowledged alerts")
    
    # Acknowledge an alert
    if alerts:
        alert_repo.acknowledge(alerts[0].id)
    
    # Get high-priority alerts
    high_priority = alert_repo.get_by_priority(email_id=1, priority="high")
```

## 🔄 Migration Guide

### Step 1: Update Imports

```python
# Old
from database_manager import DatabaseManager

# New (backward compatible)
from database import Database

# New (recommended)
from database import ConnectionManager, AdminUserRepository
```

### Step 2: Update Connection Handling

```python
# Old
db = DatabaseManager()
db.connect()
try:
    result = db.some_method()
finally:
    db.close()

# New (facade - backward compatible)
with Database() as db:
    result = db.some_method()

# New (repositories - recommended)
with ConnectionManager() as db:
    repo = SomeRepository(db)
    result = repo.some_method()
```

### Step 3: Update Method Calls

Most methods work the same, but return types are improved:

```python
# Admin operations
# Old: db.verify_admin_user(username, password)
# New: db.admin.verify_credentials(username, password)
# Returns: AdminUser object instead of dict

# Device operations
# Old: db.get_device_by_email_address(email)
# New: db.devices.get_by_email(email)
# Returns: Device object instead of just ID

# Metrics
# Old: db.get_daily_summaries(device_id, start, end)
# New: db.metrics.get_daily_summaries(device_id, start, end)
# Returns: List[DailySummary] instead of List[Tuple]
```

### Step 4: Handle Return Types

```python
# Old
user_data = db.get_admin_user_by_id(1)
if user_data:
    username = user_data["username"]

# New (facade - compatible)
user_data = db.get_admin_user_by_id(1)
if user_data:
    username = user_data["username"]

# New (repositories - better)
user = admin_repo.get_by_id(1)
if user:
    username = user.username  # Type-safe attribute access
```

## 🧪 Testing

Each repository can be tested independently:

```python
import unittest
from unittest.mock import Mock
from database.repositories.admin_repository import AdminUserRepository

class TestAdminRepository(unittest.TestCase):
    def setUp(self):
        self.mock_db = Mock()
        self.repo = AdminUserRepository(self.mock_db)
    
    def test_get_by_id_returns_user(self):
        # Mock database response
        self.mock_db.execute_query.return_value = [
            (1, 'admin', 'Admin User', 'admin@example.com', None, None, True)
        ]
        
        user = self.repo.get_by_id(1)
        
        self.assertIsNotNone(user)
        self.assertEqual(user.username, 'admin')
        self.assertEqual(user.full_name, 'Admin User')
```

## 📊 Comparison: Before vs After

| Aspect | Before | After |
|--------|--------|-------|
| **File size** | 800+ lines | <250 lines per file |
| **Responsibilities** | 50+ methods in one class | 6 focused repositories |
| **Return types** | Tuples & dicts | Type-safe dataclasses |
| **Testability** | Hard to mock | Easy to test |
| **Navigation** | Find in 800 lines | Find by domain |
| **Type hints** | Generic types | Specific models |

## 🎓 Best Practices

### 1. Use Context Managers
Always use context managers for automatic cleanup:

```python
# Good
with ConnectionManager() as db:
    repo = AdminUserRepository(db)
    user = repo.get_by_id(1)

# Avoid
db = ConnectionManager()
db.connect()
user = repo.get_by_id(1)  # Forgot to close!
```

### 2. Create Repositories Once
Reuse repository instances within a context:

```python
# Good
with ConnectionManager() as db:
    admin_repo = AdminUserRepository(db)
    device_repo = DeviceRepository(db)
    
    user = admin_repo.get_by_id(1)
    devices = device_repo.get_by_admin_user(user.id)

# Avoid creating repos repeatedly in loops
```

### 3. Use Type Hints
The models provide excellent autocomplete:

```python
from database.models import Device

def process_device(device: Device) -> None:
    # IDE will autocomplete device.email_address, device.device_type, etc.
    print(device.email_address)
```

### 4. Batch Operations
Use batch methods when available:

```python
# Good - single database round trip
metrics_repo.insert_daily_summary(device_id, date, steps=1000, calories=500)

# Avoid - multiple round trips in a loop
for metric in metrics:
    metrics_repo.insert_intraday_metric(...)
```

## 🔒 Security Notes

- Passwords are still hashed with bcrypt
- Tokens are still encrypted/decrypted
- All queries use parameterized statements
- No SQL injection vulnerabilities

## 🚧 Backward Compatibility

The `Database` facade class maintains 100% backward compatibility with the original `DatabaseManager`. Existing code will continue to work without changes.

## 📝 Next Steps

1. **Gradual Migration**: Start using repositories in new code
2. **Refactor Incrementally**: Update existing code module by module
3. **Add Tests**: Write tests for each repository
4. **Remove Facade**: Eventually remove the facade when all code is migrated

## 🤝 Contributing

When adding new database functionality:

1. Add the method to the appropriate repository
2. Add corresponding model if needed
3. Update the facade for backward compatibility
4. Write tests
5. Update this README

---

**Questions?** Open an issue or contact the development team.
