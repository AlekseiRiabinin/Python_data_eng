import csv
import json
import time
import logging
from typing import Any, Self, TypedDict
from datetime import datetime


class ETLLog(TypedDict):
    timestamp: str
    level: str
    message: str
    metadata: dict[str, Any]


class ETLExtractor:
    """Data extraction with retries and logging."""
    
    def __init__(self: Self) -> None:
        self.logs: list[ETLLog] = []
        self._setup_logging()
    
    def _setup_logging(self: Self) -> None:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('ETL')
    
    def _log(
            self: Self,
            message: str,
            level: str = "INFO",
            **metadata: dict[str, Any]
    ) -> None:
        entry = {
            "timestamp": datetime.isoformat(),
            "level": level,
            "message": message,
            "metadata": metadata
        }
        self.logs.append(entry)
        getattr(self.logger, level.lower())(f"{message} | Metadata: {metadata}")
    
    def extract_csv_with_retry(
        self: Self,
        file_path: str,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> list[dict[str, Any]]:
        """Extract data from CSV with retry mechanism."""
        attempt = 0
        while attempt <= max_retries:
            try:
                with open(file_path, mode='r') as f:
                    reader = csv.DictReader(f)
                    data = list(reader)
                    self._log(
                        "CSV extraction successful",
                        metadata={"file": file_path, "rows": len(data)}
                    )
                    return data
            except Exception as e:
                attempt += 1
                if attempt > max_retries:
                    self._log(
                        "CSV extraction failed",
                        level="ERROR",
                        metadata={
                            "file": file_path,
                            "error": str(e),
                            "attempt": attempt
                        }
                    )
                    raise
                
                self._log(
                    f"Retrying CSV extraction (attempt {attempt}/{max_retries})",
                    level="WARNING",
                    metadata={"error": str(e)}
                )
                time.sleep(retry_delay * attempt)


class ETLTransformer:
    """Data transformation with validation"""
    
    def __init__(self):
        self.logs: list[ETLLog] = []
    
    def transform_data(
        self: Self,
        data: list[dict[str, Any]],
        rules: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Apply transformation rules to data."""
        transformed = []
        for row in data:
            try:
                new_row = {}
                for field, rule in rules.items():
                    if field in row:
                        new_row[field] = self._apply_rule(row[field], rule)
                transformed.append(new_row)
            except Exception as e:
                self.logs.append({
                    "timestamp": datetime.isoformat(),
                    "level": "ERROR",
                    "message": f"Row transformation failed: {e}",
                    "metadata": {"row": row}
                })
        return transformed
    
    def _apply_rule(self: Self, value: Any, rule: dict[str, Any]) -> Any:
        """Apply single transformation rule"""
        # Example rule: {"type": "date", "format": "%Y-%m-%d"}
        if rule.get("type") == "date":
            return datetime.strptime(value, rule["format"]).date()
        elif rule.get("type") == "numeric":
            return float(value)
        return value


class ETLLoader:
    """Data loading with error handling"""
    
    def __init__(self: Self):
        self.logs: list[ETLLog] = []
    
    def load_json(
        self: Self,
        data: list[dict[str, Any]],
        output_path: str
    ) -> bool:
        """Load data to JSON file."""
        try:
            with open(output_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            self.logs.append({
                "timestamp": datetime.isoformat(),
                "level": "INFO",
                "message": "Data loaded successfully",
                "metadata": {
                    "output_path": output_path,
                    "records": len(data)
                }
            })
            return True
        except Exception as e:
            self.logs.append({
                "timestamp": datetime.isoformat(),
                "level": "ERROR",
                "message": "Data load failed",
                "metadata": {
                    "error": str(e),
                    "output_path": output_path
                }
            })
            return False
