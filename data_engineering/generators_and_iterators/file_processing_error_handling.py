import json
from typing import Iterator, Optional


def json_lines_reader(file_path: str) -> Iterator[Optional[dict]]:
    """Read JSON Lines file with proper error handling."""
    with open(file_path, 'r') as file:
        for line_number, line in enumerate(file, 1):
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                print(f"Invalid JSON at line {line_number}")
                yield None


# Usage:
# for record in json_lines_reader('data.jsonl'):
#     if record is not None:
#         process_record(record)
