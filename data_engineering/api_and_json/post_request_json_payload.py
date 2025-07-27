import requests
from typing import Any, Optional
from requests.exceptions import RequestException


def post_json_data(url: str, payload: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Send JSON data to an API endpoint."""
    try:
        response = requests.post(
            url,
            json=payload,
            headers={"Accept": "application/json"},
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        print(f"POST request failed: {e}")
        return None


# Usage:
# result = post_json_data("https://api.example.com/users", {"name": "John"})
