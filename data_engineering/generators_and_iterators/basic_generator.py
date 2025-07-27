import csv
from typing import Iterator, Any


def csv_rows_generator(file_path: str) -> Iterator[dict[str, Any]]:
    """Memory-efficient CSV reader that yields rows one at a time."""
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row


# Usage:
# for row in csv_rows_generator('large_dataset.csv'):
#     process(row)