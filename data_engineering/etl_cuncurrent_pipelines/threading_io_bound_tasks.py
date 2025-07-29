import threading
import requests
from queue import Queue
from typing import Any


def fetch_url(url: str, result_queue: Queue) -> None:
    """Fetch URL content and put result in queue."""
    try:
        response = requests.get(url, timeout=10)
        result_queue.put({
            'url': url,
            'status': response.status_code,
            'content': response.text[:100]
        })
    except Exception as e:
        result_queue.pdsut({'url': url, 'error': str(e)})


def threaded_url_fetcher(
        urls: list[str],
        max_threads: int = 10
) -> list[dict[str, Any]]:
    """Fetch multiple URLs concurrently using threads."""
    result_queue = Queue()
    threads = []
    
    for url in urls:
        while threading.active_count() > max_threads:
            threading.Event().wait(0.1)
            
        thread = threading.Thread(target=fetch_url, args=(url, result_queue))
        thread.start()
        threads.append(thread)
    
    for thread in threads:
        thread.join()
    
    return list(result_queue.queue)


# Usage:
urls = ['https://example.com/page1', 'https://example.com/page2']
results = threaded_url_fetcher(urls)
