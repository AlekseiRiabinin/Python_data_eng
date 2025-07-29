from concurrent.futures import (
     ProcessPoolExecutor,
     ThreadPoolExecutor,
     as_completed
)
from typing import Any
import time


def monitored_parallel_execution(
    tasks: list[dict[str, Any]],
    executor_type: str = 'thread',
    max_workers: int = 4
) -> dict[str, Any]:
    """Execute tasks with progress monitoring."""
    executor = (
        ThreadPoolExecutor
        if executor_type == 'thread'
        else ProcessPoolExecutor
    )
    start_time = time.time()
    stats = {
        'completed': 0,
        'failed': 0,
        'results': [],
        'durations': []
    }
    
    with executor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(task['func'],
            **task['args']): task['id'] for task in tasks
        }        
        for future in as_completed(futures):
            task_id = futures[future]
            try:
                result = future.result()
                stats['results'].append({'id': task_id, 'result': result})
                stats['completed'] += 1
            except Exception as e:
                stats['results'].append({'id': task_id, 'error': str(e)})
                stats['failed'] += 1
            finally:
                stats['durations'].append(time.time() - start_time)
    
    stats['total_time'] = time.time() - start_time
    return stats
