import json
import sqlite3
from datetime import datetime
from pathlib import Path
from contextlib import contextmanager
from typing import (
    TypedDict, Self, Protocol,
    Iterator, Literal, Optional, Any,
    runtime_checkable, overload,
)


# 1
class LogRecord(TypedDict):
    timestamp: datetime
    message: str
    severity: str


def process_error(record: LogRecord) -> None:
    """Process error records with validation and structured logging."""
    if not all(key in record for key in ('timestamp', 'message', 'severity')):
        raise ValueError(f"Invalid error record structure: {record.keys()}")

    if record['severity'] not in ('error', 'critical'):
        return

    enriched_record = {
        **record,
        'processing_time': datetime.now(),
        'source_file': record.get('source_file', 'unknown')
    }

    log_path = Path("processed_errors.jsonl")
    with log_path.open('a', encoding='utf-8') as f:
        f.write(json.dumps(enriched_record) + '\n')

    if record['severity'] == 'critical':
        notify_alert_system(enriched_record)


def notify_alert_system(record: LogRecord) -> None:
    """Mock function for alert notifications."""
    print(f"ALERT: Critical error detected - {record['message']}")


class LogStream:
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def __iter__(self) -> Iterator[LogRecord]:
        with open(self.file_path) as f:
            for line in f:
                ts, severity, msg = line.strip().split('|', 2)
                yield {
                    'timestamp': datetime.fromisoformat(ts),
                    'message': msg,
                    'severity': severity.lower()
                }


# 2
class SQLiteConnection:
    def __init__(self, db_path: str | Path) -> None:
        """Initialize with path to SQLite database."""
        self.db_path = str(db_path)
        self.conn: Optional[sqlite3.Connection] = None

    def __enter__(self) -> Self:
        """Context manager entry - establishes connection."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # Enable dict-like access
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - closes connection."""
        if self.conn:
            self.conn.close()

    def execute(
            self,
            query: str,
            params: tuple[Any, ...] = ()
    ) -> list[dict[str, Any]]:
        """Execute query and return results as dictionaries."""
        if not self.conn:
            raise RuntimeError("Connection not established")

        with self.conn:  # Auto-commits if no exceptions
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    @contextmanager
    def cursor(self) -> Any:
        """Alternative context manager for cursor operations."""
        if not self.conn:
            raise RuntimeError("Connection not established")

        cursor = self.conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()


# 3
@runtime_checkable
class Transformer(Protocol):
    def __call__(self, data: dict[str, Any]) -> dict[str, Any]:
        ...


class DataCleaner:
    def __call__(self, record: dict[str, Any]) -> dict[str, Any]:
        """Remove nulls and normalize keys."""
        return {
            k.lower(): v
            for k, v in record.items()
            if v is not None
        }


# 4
class ColumnarStorage:
    def __init__(self, data: dict[str, list[Any]]) -> None:
        self.data = data

    @overload
    def __getitem__(self, key: str) -> list[Any]: ...

    @overload
    def __getitem__(self, key: tuple[str, int]) -> Any: ...

    def __getitem__(
            self,
            key: str | tuple[str, int]
    ) -> list[Any] | Any:
        if isinstance(key, tuple):
            col, idx = key
            return self.data[col][idx]
        return self.data[key]


# 5
class Schema:
    def __init__(self, fields: dict[str, Literal["int", "str", "float"]]):
        self.fields = fields

    def __add__(self, other: Self) -> Self:
        return Schema({**self.fields, **other.fields})

    def __repr__(self) -> str:
        return f"Schema({self.fields!r})"


# Usage 1
stream = LogStream("server_logs.txt")
for record in stream:  # Uses __iter__
    if record['severity'] == 'error':
        process_error(record)  # type-checked

# Usage 2
with SQLiteConnection(":memory:") as db:
    db.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    db.execute("INSERT INTO users VALUES (?, ?)", (1, "Alice"))
    users: list[dict[str, Any]] = db.execute("SELECT * FROM users")
    print(users)  # [{'id': 1, 'name': 'Alice'}]

# Usage 3
cleaner = DataCleaner()
data = {"ID": 1, "Name": None, "Value": 42.5}
cleaned = cleaner(data)  # Uses __call__
# cleaned: dict[str, Any] is type-checked

# Usage 4
storage = ColumnarStorage({"id": [1, 2], "temp": [22.1, 23.4]})
ids: list[Any] = storage["id"]  # Full column
temp: Any = storage["temp", 1]  # Specific value

# Usage 5
user_schema = Schema({"id": "int", "name": "str"})
metrics_schema = Schema({"value": "float"})
combined = user_schema + metrics_schema  # Uses __add__
# combined: Schema = Schema({'id': 'int', 'name': 'str', 'value': 'float'})
