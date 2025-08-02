import multiprocessing
import math
import requests
import time
from typing import Any
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor


def calculate_stats(numbers: list[float]) -> dict[str, float]:
    """CPU-intensive calculations that bypass the GIL using processes."""
    return {
        'sum': sum(numbers),
        'mean': sum(numbers) / len(numbers),
        'std_dev': math.sqrt(
            sum(
                (x - (sum(numbers) / len(numbers))) ** 2 for x in numbers
            ) / len(numbers)
        )
    }


def parallel_stats_calculator(
    data_chunks: list[list[float]],
    num_processes: int = None
) -> list[dict[str, float]]:
    """Parallel processing of CPU-bound tasks across multiple cores."""
    num_processes = num_processes or multiprocessing.cpu_count()
    
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        results = list(executor.map(calculate_stats, data_chunks))
    
    return results


def fetch_url(url: str) -> dict[str, Any]:
    """I/O-bound task where GIL is less problematic."""
    try:
        response = requests.get(url, timeout=5)
        return {
            'url': url,
            'status': response.status_code,
            'content_length': len(response.text)
        }
    except Exception as e:
        return {'url': url, 'error': str(e)}


def threaded_url_fetcher(urls: list[str]) -> list[dict[str, Any]]:
    """Demonstrates GIL impact on I/O-bound tasks."""
    with ThreadPoolExecutor(max_workers=10) as executor:
        return list(executor.map(fetch_url, urls))


def cpu_bound_task(n: int) -> int:
    """GIL-bound without multiprocessing."""
    return sum(i * i for i in range(n))


def io_bound_task(url: str) -> str:
    """GIL-released during I/O waits."""
    time.sleep(1)  # Simulate I/O wait
    return f"Processed {url}"


def hybrid_worker(task: tuple[str, str, int]) -> dict[str, Any]:
    """Dispatches tasks to appropriate executor."""
    task_type, payload, size = task
    
    if task_type == 'cpu':
        return {
            'type': 'cpu',
            'result': cpu_bound_task(size),
            'input': payload
        }
    else:
        return {
            'type': 'io',
            'result': io_bound_task(payload),
            'input': payload
        }


def gil_aware_dispatcher(
        tasks: list[tuple[str, str, int]]
) -> list[dict[str, Any]]:
    """Routes tasks to threads (I/O) or processes (CPU)."""
    cpu_tasks = [t for t in tasks if t[0] == 'cpu']
    io_tasks = [t for t in tasks if t[0] == 'io']
    
    results = []
    
    # Process CPU-bound tasks
    with ProcessPoolExecutor() as cpu_executor:
        cpu_results = list(cpu_executor.map(hybrid_worker, cpu_tasks))
        results.extend(cpu_results)
    
    # Process I/O-bound tasks
    with ThreadPoolExecutor() as io_executor:
        io_results = list(io_executor.map(hybrid_worker, io_tasks))
        results.extend(io_results)
    
    return results


# Usage 1:
data = [list(range(i, i + 1000)) for i in range(0, 10000, 1000)]
stats = parallel_stats_calculator(data)
print(f"Processed {len(data)} chunks with {len(stats)} results")


# Usage 2:
urls = [
    'https://httpbin.org/get',
    'https://example.com',
    'https://google.com'
]
results2 = threaded_url_fetcher(urls)
print(f"Fetched {len([r for r in results2 if 'status' in r])} URLs successfully")


# Usage 3:
tasks = [
    ('cpu', 'calculation_1', 1000000),
    ('io', 'https://example.com', 0),
    ('cpu', 'calculation_2', 500000)
]
results3 = gil_aware_dispatcher(tasks)
print(f"Processed {len(results3)} tasks")
