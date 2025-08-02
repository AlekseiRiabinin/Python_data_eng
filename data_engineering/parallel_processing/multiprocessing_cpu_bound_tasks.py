import math
import multiprocessing
from multiprocessing import Pool
from typing import Any


def process_chunk(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Process a chunk of data (CPU-intensive operations)."""
    results = []
    for item in data:
        # Simulate CPU-bound work
        item['value_sqrt'] = math.sqrt(item['value'])
        item['value_exp'] = math.exp(item['value'])
        results.append(item)
    return results


def parallel_processing(
    data: list[dict[str, Any]],
    num_processes: int = None
) -> list[dict[str, Any]]:
    """Parallelize CPU-bound processing across multiple cores."""
    num_processes = num_processes or multiprocessing.cpu_count()
    chunk_size = len(data) // num_processes + 1
    
    with Pool(processes=num_processes) as pool:
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        results = pool.map(process_chunk, chunks)
    
    return [item for chunk in results for item in chunk]


# Usage:
data = [{'id': i, 'value': i*10} for i in range(1000)]
processed = parallel_processing(data)
