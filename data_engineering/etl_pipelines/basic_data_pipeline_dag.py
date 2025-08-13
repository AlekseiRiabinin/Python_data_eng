from datetime import datetime, timedelta
from typing import Callable, Optional, TypeVar, Self
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.context import Context
from airflow import DAG


T = TypeVar('T')  # Generic type for return values


class EnhancedPythonOperator(BaseOperator):
    """Custom Python Operator."""
    
    @apply_defaults
    def __init__(
        self: Self,
        python_callable: Callable[..., T],
        validate_return: Optional[Callable[[T], bool]] = None,
        log_return: bool = True,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.validate_return = validate_return
        self.log_return = log_return

    def execute(self: Self, context: Context) -> Optional[T]:
        """Execute operator."""
        self.log.info(f"Executing {self.python_callable.__name__}")
        
        try:
            result = self.python_callable(**context)
        except TypeError:
            result = self.python_callable()
        
        if self.validate_return and not self.validate_return(result):
            raise ValueError(
                f"Return value validation failed for "
                f"{self.python_callable.__name__}"
            )

        if self.log_return:
            self.log.info(
                f"Function {self.python_callable.__name__} "
                f"returned: {result}"
            )
        
        return result


# Example usage
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def extract_data() -> dict[str, list[int]]:
    """Mock data extraction."""
    print("Extracting data from source...")
    return {"data": [1, 2, 3, 4, 5]}


def transform_data(**context) -> list[int]:
    """Get data from previous task."""
    data = context['task_instance'].xcom_pull(task_ids='extract')
    print(f"Transforming data: {data}")
    return [x * 2 for x in data["data"]]


def load_data(**context) -> None:
    """Load data."""
    transformed_data = context['task_instance'].xcom_pull(task_ids='transform')
    print(f"Loading data: {transformed_data}")


def validate_transformed_data(data: list) -> bool:
    """Validate data."""
    return all(isinstance(x, int) for x in data)


with DAG(
    'enhanced_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract = EnhancedPythonOperator(
        task_id='extract',
        python_callable=extract_data,
        log_return=True
    )
    
    transform = EnhancedPythonOperator(
        task_id='transform',
        python_callable=transform_data,
        validate_return=validate_transformed_data,
        log_return=True
    )
    
    load = EnhancedPythonOperator(
        task_id='load',
        python_callable=load_data
    )
    
    extract >> transform >> load
