import csv
import json
import sqlite3
from pathlib import Path
from typing import Any


def read_data(source: str | Path) -> list[dict[str, Any]]:
    """Read data from CSV, JSON, or SQLite."""
    # SQLite Database
    if source.startswith('sqlite:///'):
        try:
            db_path = source.replace('sqlite:///', '')
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM data")
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            print(f"Database error: {e}")
            return []
        finally:
            conn.close()

    # CSV Files
    elif source.endswith('.csv'):
        try:
            with open(source, mode='r', encoding='utf-8') as f:
                return list(csv.DictReader(f))
        except Exception as e:
            print(f"CSV read error: {e}")
            return []

    # JSON Files
    elif source.endswith('.json'):
        try:
            with open(source, mode='r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"JSON read error: {e}")
            return []

    else:
        raise ValueError(f"Unsupported format: {source}")


def process_data(*sources: str | Path) -> list[dict[str, Any]]:
    """Merge data from multiple sources."""
    combined = []
    for src in sources:
        combined.extend(read_data(src))
    return combined
