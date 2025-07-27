from typing import Iterator, List, Dict, Any, Optional, TypedDict
import time
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from api_data_pandas_dataframe import fetch_api_data


class PaginationStats(TypedDict):
    """Type definition for pagination statistics."""
    pages_fetched: int
    items_fetched: int
    duration_sec: float
    avg_items_per_sec: float


def fetch_paginated_data(
    base_url: str,
    initial_page: int = 1,
    max_pages: Optional[int] = None,
    page_size: Optional[int] = None,
    params: Optional[dict[str, Any]] = None,
    timeout: float = 10.0,
    throttle_delay: float = 0.1
) -> Iterator[dict[str, Any]]:
    """Robust paginated API fetcher with comprehensive controls."""
    start_time = time.time()
    page = initial_page
    items_fetched = 0
    
    while max_pages is None or page <= max_pages:
        url = base_url.format(page=page, page_size=page_size)
        
        if params:
            parsed = urlparse(url)
            query = parse_qs(parsed.query)
            query.update({k: [v] for k, v in params.items()})
            url = urlunparse(parsed._replace(query=urlencode(query, doseq=True)))
        
        response = fetch_api_data(url, timeout=timeout)
        
        if not response or not response.get('data'):
            break
            
        data = response['data']
        if not isinstance(data, list):
            data = [data]
            
        if not data:
            break
            
        for item in data:
            yield {
                'data': item,
                'metadata': {
                    'page': page,
                    'page_size': page_size,
                    'response_headers': response['headers'],
                    'request_timestamp': response['timestamp']
                }
            }
            items_fetched += 1
            
        page += 1
        
        if throttle_delay > 0:
            time.sleep(throttle_delay)
    
    stats: PaginationStats = {
        'pages_fetched': page - initial_page,
        'items_fetched': items_fetched,
        'duration_sec': time.time() - start_time,
        'avg_items_per_sec': items_fetched / (time.time() - start_time) if items_fetched else 0
    }
    yield {'_stats': stats}


# Usage:
# for page in fetch_paginated_data("https://api.example.com/users?page={page}"):
#     process_users(page)
