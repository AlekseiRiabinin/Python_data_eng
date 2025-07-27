import requests
import time
import pandas as pd
from typing import Any, Optional, TypedDict
from requests.exceptions import RequestException
from datetime import datetime


class ApiResponse(TypedDict):
    """Type definition for successful API responses."""
    status: int
    headers: dict[str, str]
    data: Optional[dict[str, Any] | list]
    timestamp: str


def api_to_dataframe(
    url: str,
    params: Optional[dict[str, Any]] = None,
    json_path: Optional[list[str]] = None,
    date_columns: Optional[list[str]] = None,
    numeric_columns: Optional[list[str]] = None
) -> pd.DataFrame:
    """Fetch API data and convert to pandas DataFrame with smart type conversion."""
    response = fetch_api_data(url, params=params)
    
    if not response or not response.get('data'):
        return pd.DataFrame()
    
    data = response['data']
    if json_path:
        for key in json_path:
            data = data.get(key, {}) if isinstance(data, dict) else {}
            if not data:
                break
    
    try:
        df = pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame([data])
    except ValueError as e:
        print(f"Data conversion failed: {e}")
        return pd.DataFrame()
    
    def convert_dtypes():
        dt_cols = date_columns or [
            col for col in df.columns 
            if any(k in col.lower() for k in ['date', 'time', 'at'])
        ]
        for col in dt_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        num_cols = numeric_columns or [
            col for col in df.columns 
            if any(k in col.lower() for k in ['amount', 'price', 'value'])
        ]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
    
    convert_dtypes()
    
    df.attrs['api_response'] = {
        'status': response['status'],
        'headers': response['headers'],
        'timestamp': response['timestamp']
    }
    return df


def fetch_api_data(
    url: str,
    method: str = "GET",
    params: Optional[dict[str, Any]] = None,
    json_data: Optional[dict[str, Any]] = None,
    headers: Optional[dict[str, str]] = None,
    timeout: int = 10,
    max_retries: int = 3,
    backoff_factor: float = 0.5
) -> Optional[ApiResponse]:
    """Robust API client with retry mechanism and full typing support."""
    valid_methods = {"GET", "POST", "PUT", "DELETE", "PATCH"}
    if method.upper() not in valid_methods:
        raise ValueError(f"Invalid method: {method}. Must be one of {valid_methods}")

    default_headers = {
        "Accept": "application/json",
        "User-Agent": "DataEngineeringBot/1.0"
    }
    headers = {**default_headers, **(headers or {})}

    session = requests.Session()
    retry_count = 0

    while retry_count <= max_retries:
        try:
            response = session.request(
                method=method,
                url=url,
                params=params,
                json=json_data,
                headers=headers,
                timeout=timeout
            )
            
            # Successful response
            return {
                "status": response.status_code,
                "headers": dict(response.headers),
                "data": response.json() if response.content else None,
                "timestamp": datetime.utcnow().isoformat()
            }

        except RequestException as e:
            retry_count += 1
            if retry_count > max_retries:
                print(f"Max retries exceeded for {url}: {e}")
                return None
            
            # Exponential backoff
            sleep_time = backoff_factor * (2 ** (retry_count - 1))
            time.sleep(sleep_time)


# Usage:
# df = api_to_dataframe("https://api.example.com/transactions")
