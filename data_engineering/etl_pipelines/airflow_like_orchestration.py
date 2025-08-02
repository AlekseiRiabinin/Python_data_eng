import time
from typing import Callable, Any, Self
from core_etl_pipeline import ETLExtractor, ETLTransformer, ETLLoader 


class DAG:
    """Simple DAG (Directed Acyclic Graph) implementation."""
    
    def __init__(self: Self, dag_id: str):
        self.tasks = []
        self.dag_id = dag_id

    def task(self: Self, task_id: str, retries: int = 0) -> Callable:
        """Decorator to register tasks."""
        def decorator(func: Callable) -> Callable:
            self.tasks.append({
                "task_id": task_id,
                "function": func,
                "retries": retries
            })
            return func
        return decorator
    
    def run(self: Self) -> dict[str, Any]:
        """Execute all tasks in order."""
        results = {}
        for task in self.tasks:
            task_id = task["task_id"]
            print(f"Running task: {task_id}")
            
            attempt = 0
            while attempt <= task["retries"]:
                try:
                    results[task_id] = task["function"]()
                    break
                except Exception:
                    attempt += 1
                    if attempt > task["retries"]:
                        print(f"Task {task_id} failed after {task['retries']} retries")
                        raise
                    print(f"Retrying task {task_id} (attempt {attempt})")
                    time.sleep(2 ** attempt)
        
        return results


# Example Usage:
etl_dag = DAG(dag_id="sample_etl")


@etl_dag.task(task_id="extract_data", retries=2)
def extract():
    extractor = ETLExtractor()
    return extractor.extract_csv_with_retry("input.csv")


@etl_dag.task(task_id="transform_data")
def transform(extract_result):
    transformer = ETLTransformer()
    rules = {
        "date": {"type": "date", "format": "%Y-%m-%d"},
        "amount": {"type": "numeric"}
    }
    return transformer.transform_data(extract_result, rules)


@etl_dag.task(task_id="load_data", retries=1)
def load(transform_result):
    loader = ETLLoader()
    success = loader.load_json(transform_result, "output.json")
    if not success:
        raise ValueError("Load operation failed")
    return "Data loaded successfully"


# Manual dependency resolution
def run_etl():
    extract_result = extract()
    transform_result = transform(extract_result)
    return load(transform_result)


# Or use the DAG runner
if __name__ == "__main__":
    etl_dag.run()
