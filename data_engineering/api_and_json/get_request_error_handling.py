import requests
from typing import Any, Optional
from requests.exceptions import RequestException


def fetch_api_data(
        url: str,
        params: Optional[dict[str, Any]] = None
) -> Optional[dict[str, Any]]:
    """Fetch JSON data from a REST API with proper error handling."""
    try:
        response = requests.get(
            url,
            params=params,
            headers={"Accept": "application/json"},
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        print(f"API request failed: {e}")
        return None


# Usage:
# user_data = fetch_api_data("https://jsonplaceholder.typicode.com/users/1")
