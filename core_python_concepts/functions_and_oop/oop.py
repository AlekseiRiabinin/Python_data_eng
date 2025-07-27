import sqlite3
from typing import Protocol, Any, TypedDict, ClassVar, runtime_checkable
from dataclasses import dataclass
from collections.abc import Iterator


class DBConfig(TypedDict):
    database: str
    timeout: float | None


class KafkaConfig(TypedDict):
    bootstrap_servers: str
    group_id: str | None


@runtime_checkable
class Connection(Protocol):
    def close(self) -> None: ...
    def commit(self) -> None: ...


class Consumer(Protocol):
    def poll(self, timeout_ms: int) -> Iterator[dict[str, Any]]: ...


class DataConnector(Protocol):
    source: DBConfig | KafkaConfig

    def connect(self) -> Connection | Consumer:
        """Connect to data source."""
        ...


# Implementations
@dataclass
class SQLiteConnector:
    source: DBConfig

    def connect(self) -> sqlite3.Connection:
        """Connect to SQLite database."""
        return sqlite3.connect(
            database=self.source['database'],
            timeout=self.source.get('timeout', 5.0)
        )


@dataclass
class MockKafkaConnector:
    source: KafkaConfig

    def connect(self) -> Consumer:
        """Mock Kafka consumer."""
        class MockConsumer:
            def __init__(self, config: KafkaConfig):
                self.config = config

            def poll(self, timeout_ms: int = 1000) -> Iterator[dict[str, Any]]:
                # Simulate receiving messages
                yield {"topic": "sales", "value": b"data"}

        return MockConsumer(self.source)


# Usage with type checking
def run_pipeline(connector: DataConnector) -> None:
    with connector.connect() as conn:
        if isinstance(conn, sqlite3.Connection):
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM sales")
            print(cursor.fetchall())
        else:  # Kafka consumer
            for msg in conn.poll():
                print(msg['value'])


# Usage of Class variable
class DatabaseConfig:
    # Class variable (shared across all instances)
    DEFAULT_TIMEOUT: ClassVar[int] = 30

    def __init__(self, connection_url: str) -> None:
        # Instance variable (unique to each instance)
        self.connection_url = connection_url
        self.timeout = self.DEFAULT_TIMEOUT


# Example 1
sqlite_config: DBConfig = {"database": ":memory:", "timeout": 10.0}
kafka_config: KafkaConfig = {"bootstrap_servers": "localhost:9092"}

run_pipeline(SQLiteConnector(sqlite_config))
run_pipeline(MockKafkaConnector(kafka_config))

# Example 2
config1 = DatabaseConfig("postgres://user1@db1")
config2 = DatabaseConfig("postgres://user2@db2")

print(config1.DEFAULT_TIMEOUT)  # 30 (class variable)
print(config2.DEFAULT_TIMEOUT)  # 30 (same class variable)

config1.timeout = 60  # Only affects config1
print(config1.timeout)  # 60 (instance-specific)
print(config2.timeout)  # 30 (still using class default)
