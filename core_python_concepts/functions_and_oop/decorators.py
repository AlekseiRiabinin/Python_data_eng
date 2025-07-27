from __future__ import annotations
from typing import Callable, TypeVar, ParamSpec, Any
import time

P = ParamSpec('P')  # For args/kwargs
R = TypeVar('R')    # For return type


# 1
def log_execution_time(func: Callable[P, R]) -> Callable[P, R]:
    """Decorator to log function runtime - critical for ETL jobs."""
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start = time.time()
        result = func(*args, **kwargs)
        print(f"{func.__name__} executed in {time.time() - start:.2f}s")
        return result
    return wrapper


@log_execution_time
def clean_dataset(df: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Data cleaning function."""
    return [row for row in df if all(val is not None for val in row.values())]


# 2
class DataPipeline:
    def __init__(self, batch_size: int) -> None:
        self._batch_size = batch_size

    @property
    def batch_size(self) -> int:
        return self._batch_size

    @batch_size.setter
    def batch_size(self, value: int) -> None:
        if not 100 <= value <= 10_000:
            raise ValueError("Batch size must be 100-10,000")
        self._batch_size = value


# 3
class DataLoader:
    def __init__(self, file_path: str) -> None:
        self.data = self._load(file_path)

    @classmethod
    def from_s3(cls, bucket: str, key: str) -> DataLoader:
        """Alternative constructor for S3 data."""
        s3_path = f"s3://{bucket}/{key}"
        return cls(s3_path)

    def _load(self, path: str) -> object:
        # Implementation omitted
        pass


# Usage example 1
data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": None}]
cleaned = clean_dataset(data)

# Usage example 2
pipeline = DataPipeline(500)
pipeline.batch_size = 2000  # Raises ValueError

# Usage example 3
loader1 = DataLoader("local.csv")
loader2 = DataLoader.from_s3("my-bucket", "data/2023.csv")
