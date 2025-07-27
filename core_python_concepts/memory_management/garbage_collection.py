import gc
import json
import gzip
from typing import Iterator, Dict, Any
from datetime import datetime
from pathlib import Path


class ErrorRecord(Dict[str, Any]):
    """Type hint for error records with mandatory fields."""
    timestamp: str
    message: str
    severity: str
    service: str


def stream_json_file(
    file_path: str,
    gc_interval: int = 10_000  # Manually trigger GC every N records
) -> Iterator[Dict[str, Any]]:
    """Memory-efficient JSONL streamer with controlled GC."""
    record_count = 0

    with open(file_path) as f:
        for line in f:
            yield json.loads(line)
            record_count += 1

            # Manual GC trigger at intervals
            if gc_interval > 0 and record_count % gc_interval == 0:
                gc.collect()  # Explicit cleanup


def process_error(
    record: ErrorRecord,
    error_log_path: Path = Path("errors.jsonl.gz"),
    max_file_size: int = 100 * 1024 * 1024  # 100MB per file
) -> None:
    """Process and compress error records with rotation."""
    if not all(k in record for k in ("timestamp", "message", "severity")):
        raise ValueError(
            f"Invalid error record: missing fields in {record.keys()}"
        )
    enriched_record = {
        **record,
        "_processed_at": datetime.isoformat(),
        "_host": "etl-worker-1"  # In production, use socket.gethostname()
    }
    current_file = get_current_file(error_log_path, max_file_size)

    with gzip.open(current_file, "at", encoding="utf-8") as f:
        f.write(json.dumps(enriched_record) + "\n")

    if record["severity"] == "CRITICAL":
        notify_alert_system(enriched_record)


def get_current_file(base_path: Path, max_size: int) -> Path:
    """Implement log rotation based on file size."""
    counter = 0
    while True:
        current_file = base_path.with_suffix(f".{counter}.jsonl.gz")
        if not current_file.exists():
            return current_file
        if current_file.stat().st_size < max_size:
            return current_file
        counter += 1


def notify_alert_system(record: ErrorRecord) -> None:
    """Mock function for alerting (in prod: Slack/PD/webhook)."""
    print(
         "CRITICAL ERROR @ "
        f"{record['timestamp']}: "
        f"{record['message'][:100]}..."
    )


# Usage: Process with manual GC every 50k records
for record in stream_json_file("bigdata_logs.jsonl", gc_interval=50_000):
    process_error(record)
