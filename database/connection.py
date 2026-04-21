"""
Database access via SQLAlchemy.

Repositories use SqlalchemyConnection.execute_query(), which mirrors the legacy
psycopg2 API (percent-style placeholders) while running through SQLAlchemy 2.
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple, Union

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

RowList = List[Tuple[Any, ...]]


def _percent_to_named(query: str, params: Tuple[Any, ...]) -> Tuple[Any, dict]:
    parts = query.split("%s")
    if len(parts) - 1 != len(params):
        raise ValueError(
            f"Placeholder count ({len(parts) - 1}) does not match "
            f"parameter count ({len(params)})"
        )
    bind: dict = {}
    fragments: List[str] = []
    for i, param in enumerate(params):
        key = f"p{i}"
        bind[key] = param
        fragments.append(parts[i])
        fragments.append(f":{key}")
    fragments.append(parts[-1])
    return text("".join(fragments)), bind


class _CursorDescription:
    """Mimic psycopg2 cursor.description for repositories that read column names."""

    def __init__(self, keys: List[str]) -> None:
        self.description = [(k,) for k in keys]


class SqlalchemyConnection:
    """
    Connection wrapper passed to repositories (legacy execute_query surface).

    Attributes:
        session: Active SQLAlchemy Session.
        cursor: Set after SELECT/RETURNING queries to expose .description.
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self.cursor: Optional[_CursorDescription] = None

    def execute_query(
        self,
        query: str,
        params: Optional[Tuple[Any, ...]] = None,
    ) -> Union[RowList, bool, None]:
        params = params or ()
        self.cursor = None
        try:
            if "%s" in query:
                stmt, bind = _percent_to_named(query, params)
            else:
                stmt = text(query)
                bind = {}

            result = self.session.execute(stmt, bind)

            if result.returns_rows:
                keys = list(result.keys())
                self.cursor = _CursorDescription(keys)
                rows = result.fetchall()
                self.session.commit()
                return [tuple(row) for row in rows]

            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            print(f"Error executing query: {e}")
            return None

    def execute_many(self, query: str, params_list: List[Tuple[Any, ...]]) -> bool:
        try:
            for params in params_list:
                if "%s" in query:
                    stmt, bind = _percent_to_named(query, params)
                else:
                    stmt = text(query)
                    bind = {}
                self.session.execute(stmt, bind)
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            print(f"Error executing multiple queries: {e}")
            return False


_engine = None
_SessionFactory: Optional[sessionmaker] = None


def _get_script_session() -> Session:
    """Session for standalone scripts (no Flask app context)."""
    global _engine, _SessionFactory
    if _engine is None:
        from urllib.parse import quote_plus

        from config import DB_CONFIG

        url = (
            f"postgresql+psycopg2://{DB_CONFIG['user']}:{quote_plus(DB_CONFIG['password'])}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        _engine = create_engine(
            url,
            pool_pre_ping=True,
            connect_args={"sslmode": DB_CONFIG.get("sslmode", "require")},
        )
        _SessionFactory = sessionmaker(bind=_engine, autoflush=False, autocommit=False)
    assert _SessionFactory is not None
    return _SessionFactory()


class ConnectionManager:
    """
    Context manager yielding SqlalchemyConnection.

    Inside a Flask application context, uses ``db.session``.
    Outside Flask (e.g. legacy scripts), creates a dedicated SQLAlchemy session
    using ``config.DB_CONFIG`` (call ``load_dotenv()`` before importing config).
    """

    def __init__(self) -> None:
        self._script_session: Optional[Session] = None
        self._owns_script_session = False

    def __enter__(self) -> SqlalchemyConnection:
        try:
            from flask import has_app_context

            if has_app_context():
                from extensions import db

                return SqlalchemyConnection(db.session)
        except Exception:
            pass

        self._script_session = _get_script_session()
        self._owns_script_session = True
        return SqlalchemyConnection(self._script_session)

    def __exit__(self, exc_type, exc, exc_tb) -> None:
        try:
            from flask import has_app_context

            if has_app_context():
                from extensions import db

                if exc_type is not None:
                    db.session.rollback()
                return
        except Exception:
            pass

        if self._owns_script_session and self._script_session is not None:
            if exc_type is not None:
                self._script_session.rollback()
            else:
                self._script_session.commit()
            self._script_session.close()
            self._script_session = None
            self._owns_script_session = False
