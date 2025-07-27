from __future__ import annotations
from typing import Optional, Any, Dict, List, Iterator
import sqlite3
import contextlib
from dataclasses import dataclass


# ====================
# Core Connection Class
# ====================
class DatabaseConnection:
    _instance: Optional[DatabaseConnection] = None
    conn: Any

    def __new__(cls, *args: Any, **kwargs: Any) -> DatabaseConnection:
        """Singleton pattern implementation."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, connection_string: str) -> None:
        """Initialize connection if not already established."""
        if not hasattr(self, 'conn'):
            self.conn = create_connection(connection_string)

    @contextlib.contextmanager
    def cursor(self) -> Iterator[Any]:
        """Context manager for database cursor (proper resource cleanup)."""
        cursor = self.conn.cursor()
        try:
            yield cursor
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cursor.close()

    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query and return results as dictionaries."""
        with self.cursor() as cur:
            cur.execute(query, params or {})
            if cur.description:
                columns = [col[0] for col in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
            return []

    def batch_insert(self, table: str, data: List[Dict[str, Any]]) -> int:
        """Bulk insert records with parameterized queries."""
        if not data:
            return 0

        with self.cursor() as cur:
            columns = list(data[0].keys())
            placeholders = ', '.join(['%s'] * len(columns))
            query = f"""
                INSERT INTO {table}
                    ({', '.join(columns)})
                VALUES
                    ({placeholders})
            """
            cur.executemany(query, [tuple(item.values()) for item in data])
            return len(data)


# ====================
# Helper Functions
# ====================
def create_connection(connection_string: str) -> Any:
    """Factory for database connections."""
    if connection_string.startswith('sqlite://'):
        return sqlite3.connect(connection_string.replace('sqlite://', ''))
    elif connection_string.startswith('postgres://'):
        raise NotImplementedError("Postgres support would require psycopg2")
    else:
        raise ValueError(f"Unsupported connection string: {connection_string}")


# ====================
# Data Engineering Service
# ====================
@dataclass
class DataPipeline:
    """Service class for data pipeline operations."""
    db: DatabaseConnection

    def process_incremental_load(
            self,
            source_table: str,
            target_table: str,
            watermark_col: str
    ) -> int:
        """Process only new/changed records since last run."""
        max_watermark = self.db.execute_query(
            f"SELECT MAX({watermark_col}) FROM {target_table}"
        )[0].get(watermark_col)

        query = f"""
            SELECT * FROM {source_table}
            WHERE {watermark_col} > %(watermark)s
            ORDER BY {watermark_col}
        """
        new_records = self.db.execute_query(
            query, {'watermark': max_watermark}
        )

        if new_records:
            self.db.batch_insert(target_table, new_records)
        return len(new_records)

    def backfill_partition(self, table: str, partition_value: str) -> None:
        """Reprocess data for a specific partition."""
        self.db.execute_query(f"""
            DELETE FROM {table}
            WHERE partition_column = %(partition)s
        """, {'partition': partition_value})

        # Call data transformation logic here
        transformed = [...]  # Transformation pipeline
        self.db.batch_insert(table, transformed)


# ====================
# Usage Examples
# ====================
if __name__ == "__main__":
    # Singleton behavior test
    db1 = DatabaseConnection("sqlite://:memory:")
    db2 = DatabaseConnection("sqlite://different_path.db")
    print(db1 is db2)  # True - singleton works

    # Initialize schema
    db1.execute_query("CREATE TABLE test (id INTEGER, name TEXT)")

    # Batch insert
    records = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    db1.batch_insert("test", records)

    # Query example
    query = """
        SELECT *
        FROM test
        WHERE id > %(min_id)s
    """

    params = {
        'min_id': 0  # Parameter value
    }

    results = db1.execute_query(query, params)
    print(results)  # [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]

    # Pipeline service example
    pipeline = DataPipeline(db1)
    pipeline.process_incremental_load("source", "target", "updated_at")
