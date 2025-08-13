import requests
from typing import Any, Optional, Callable, Self
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow import DAG
from datetime import datetime, timedelta


class CustomHttpSensor(BaseOperator):
    """Custom HTTP Sensor that pings an endpoint until it's available."""
    
    @apply_defaults
    def __init__(
        self: Self,
        endpoint: str,
        request_params: Optional[dict] = None,
        headers: Optional[dict] = None,
        response_check: Optional[Callable] = None,
        extra_options: Optional[dict] = None,
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.request_params = request_params or {}
        self.headers = headers or {}
        self.response_check = response_check
        self.extra_options = extra_options or {}

    def poke(self: Self, context: Any) -> bool:
        """Check the response."""
        try:
            response = requests.get(
                self.endpoint,
                params=self.request_params,
                headers=self.headers,
                **self.extra_options
            )

            if self.response_check:
                return self.response_check(response)
            
            return response.status_code == 200
            
        except requests.exceptions.RequestException as e:
            self.log.warning(f"HTTP request failed: {e}")
            return False

    def execute(self: Self, context: Any):
        """Overriding execute to use poke for sensor behavior."""
        from airflow.sensors.base import poke_mode_only
        return poke_mode_only(self, context)


class CustomHttpOperator(BaseOperator):
    """Custom HTTP Operator that makes HTTP requests."""
    
    @apply_defaults
    def __init__(
        self: Self,
        endpoint: str,
        method: str = 'GET',
        data: Optional[dict] = None,
        headers: Optional[dict] = None,
        response_filter: Optional[Callable] = None,
        extra_options: Optional[dict] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.method = method.upper()
        self.data = data or {}
        self.headers = headers or {}
        self.response_filter = response_filter
        self.extra_options = extra_options or {}

    def execute(self: Self, context: Any):
        """Create a response."""
        try:
            if self.method == 'GET':
                response = requests.get(
                    self.endpoint,
                    params=self.data,
                    headers=self.headers,
                    **self.extra_options
                )
            elif self.method == 'POST':
                response = requests.post(
                    self.endpoint,
                    json=self.data,
                    headers=self.headers,
                    **self.extra_options
                )
            else:
                raise AirflowException(f"Unsupported HTTP method: {self.method}")
            
            response.raise_for_status()
            
            if self.response_filter:
                return self.response_filter(response)
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            raise AirflowException(f"HTTP request failed: {e}")


def process_api_response(
        response: requests.Response
) -> list[dict[str, Any]] | dict[str, Any]:
    """Process an API response and return parsed JSON data."""
    data: list[dict[str, Any]] | dict[str, Any] = response.json()
    print(f"Processed {len(data) if isinstance(data, list) else 1} records")
    return data


# Example DAG using the custom operators
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'custom_api_data_ingestion',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
) as dag:
    
    check_api = CustomHttpSensor(
        task_id='check_api_available',
        endpoint='https://api.openweathermap.org/data/2.5/forecast',
        request_params={'q': 'Berlin', 'appid': 'your_api_key'},
        response_check=lambda response: response.status_code == 200,
        poke_interval=30,
        timeout=60*60,
        mode='poke'
    )

    fetch_data = CustomHttpOperator(
        task_id='fetch_weather_data',
        endpoint='https://api.openweathermap.org/data/2.5/forecast',
        method='GET',
        data={'q': 'Berlin', 'appid': 'your_api_key'},
        response_filter=process_api_response
    )
    
    check_api >> fetch_data
