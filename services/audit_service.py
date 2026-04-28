import hashlib
import json
from datetime import datetime, timezone
from database.orm_models import AuditLogModel


class AuditService:
    def __init__(self, db_connection):
        self.db = db_connection
        self.session = db_connection.session

    def _calculate_hash(self, log_entry, previous_hash):
        content = f"{log_entry.actor_id}{log_entry.action}{log_entry.resource_type}{log_entry.resource_id}{log_entry.timestamp.isoformat()}{previous_hash or ''}"
        return hashlib.sha256(content.encode()).hexdigest()[:64]

    def _get_last_audit_log(self):
        return self.session.query(AuditLogModel).order_by(AuditLogModel.id.desc()).first()

    def log_event(self, actor_id, actor_role, action, resource_type, resource_id=None, outcome='success', details=None, ip_address=None, user_agent=None):
        last_log = self._get_last_audit_log()
        prev_hash = last_log.previous_hash if last_log else None

        timestamp = datetime.now(timezone.utc)

        log_entry = AuditLogModel(
            actor_id=actor_id,
            actor_role=actor_role,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            outcome=outcome,
            details=details,
            ip_address=ip_address,
            user_agent=user_agent,
            timestamp=timestamp,
            previous_hash=prev_hash
        )

        log_entry.previous_hash = self._calculate_hash(log_entry, prev_hash)

        self.session.add(log_entry)
        self.session.commit()

        return log_entry