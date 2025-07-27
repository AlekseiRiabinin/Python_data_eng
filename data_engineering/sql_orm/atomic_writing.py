from pathlib import Path
import os
import json
import sqlite3


# 1
def write_atomic(filepath: Path, data: dict) -> None:
    """Atomic write using temp file + rename (POSIX compliant)."""
    temp_path = filepath.with_suffix(".tmp")

    try:
        # 1. Write to temp file
        with open(temp_path, "w") as f:
            json.dump(data, f)

        # 2. fsync to force write to disk
        os.fsync(f.fileno())

        # 3. Rename temp → final (atomic on POSIX)
        os.replace(temp_path, filepath)
    finally:
        # Clean up temp file if something failed
        if temp_path.exists():
            temp_path.unlink()


# 2
def atomic_db_insert(db_path: str, records: list[dict]) -> None:
    """Transaction ensures all or nothing write."""
    conn = sqlite3.connect(db_path)
    try:
        with conn:  # BEGIN TRANSACTION
            conn.executemany(
                "INSERT INTO logs VALUES (:timestamp, :message)",
                records
            )
        # COMMIT happens automatically if no exception
    except sqlite3.Error:
        # ROLLBACK happens automatically
        raise


# Usage 1
write_atomic(Path("data.json"), {"key": "value"})

# Usage 2
atomic_db_insert("logs.db", [{"timestamp": "2023-01-01", "message": "error"}])
