from typing import Generator
import csv
import json


def csv_to_jsonl_stream(input_path: str) -> Generator[str, None, int]:
    """Convert CSV to JSONL with error counting."""
    error_count = 0
    try:
        with open(input_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "error" in row:
                    error_count += 1
                yield json.dumps(row) + "\n"
    finally:
        return error_count


# Usage
gen = csv_to_jsonl_stream("data.csv")
try:
    with open("output.jsonl", "w") as f:
        for json_line in gen:
            f.write(json_line)
except StopIteration as e:
    print(f"Total errors: {e.value}")
