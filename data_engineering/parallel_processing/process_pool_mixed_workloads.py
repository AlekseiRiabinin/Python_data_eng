from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any
import os


def mixed_workload_processing(
    tasks: list[dict[str, Any]],
    cpu_bound_threshold: int = 1000
) -> list[dict[str, Any]]:
    """Hybrid approach using both processes and threads."""
    cpu_tasks = []
    io_tasks = []
    
    for task in tasks:
        if task.get('complexity', 0) > cpu_bound_threshold:
            cpu_tasks.append(task)
        else:
            io_tasks.append(task)
    
    results = []
    
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        cpu_results = list(executor.map(process_cpu_task, cpu_tasks))
        results.extend(cpu_results)
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        io_results = list(executor.map(process_io_task, io_tasks))
        results.extend(io_results)
    
    return results


def process_cpu_task(task: dict[str, Any]) -> dict[str, Any]:
    """Simulate CPU-intensive processing."""
    return {'id': task['id'], 'result': task['data'] ** 2}

def process_io_task(task: dict[str, Any]) -> dict[str, Any]:
    """Simulate I/O-bound processing."""
    return {'id': task['id'], 'result': len(task['data'])}


# Usage:
tasks = [{'id': i, 'data': 'x' * i, 'complexity': i * 100} for i in range(1, 50)]
processed = mixed_workload_processing(tasks)
