import time
import concurrent.futures
from typing import Any, Optional, TypedDict
from collections import defaultdict
from api_data_pandas_dataframe import fetch_api_data


class BatchApiResponse(TypedDict):
    """Type definition for batch API results."""
    successes: list[dict[str, Any]]
    failures: list[dict[str, Any]]
    stats: dict[str, float]


def fetch_multiple_endpoints(
    urls: list[str],
    params_list: Optional[list[Optional[dict[str, Any]]]] = None,
    max_workers: int = 5,
    timeout_per_request: float = 10.0,
    overall_timeout: float = 30.0
) -> BatchApiResponse:
    """Fetch multiple API endpoints in parallel with enhanced controls."""
    start_time = time.time()
    results = defaultdict(list)
    params_list = params_list or [None] * len(urls)
    
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers
    ) as executor:
        future_to_url = {
            executor.submit(
                fetch_api_data,
                url=url,
                params=params,
                timeout=timeout_per_request
            ): (url, params)
            for url, params in zip(urls, params_list)
        }
        
        for future in concurrent.futures.as_completed(
            future_to_url,
            timeout=overall_timeout
        ):
            url, params = future_to_url[future]
            try:
                response = future.result()
                if response and response.get('data'):
                    results['successes'].append(response)
                else:
                    results['failures'].append({
                        'url': url,
                        'params': params,
                        'error': 'Empty or invalid response'
                    })
            except Exception as e:
                results['failures'].append({
                    'url': url,
                    'params': params,
                    'error': str(e)
                })
    
    results['stats'] = {
        'total_requests': len(urls),
        'success_rate': len(results['successes']) / len(urls) * 100,
        'duration_sec': round(time.time() - start_time, 2),
        'requests_per_sec': round(len(urls) / (time.time() - start_time), 2)
    }
    
    return dict(results)


# Usage:
# results = fetch_multiple_endpoints([
#     "https://api.example.com/users/1",
#     "https://api.example.com/users/2"
# ])
