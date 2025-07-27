import json
import requests
from typing import Iterator, Any


def stream_large_json(url: str) -> Iterator[dict[str, Any]]:
    """Stream and parse large JSON responses line by line."""
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        for line in response.iter_lines():
            if line:
                yield json.loads(line)


# Usage:
# for record in stream_large_json("https://api.example.com/large-dataset"):
#     process_record(record)
