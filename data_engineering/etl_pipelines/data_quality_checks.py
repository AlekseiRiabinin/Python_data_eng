from datetime import datetime, timedelta
from typing import Any, Optional, Self
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook
from airflow import DAG
import great_expectations as ge
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult
)
from sqlalchemy import create_engine


class CustomDataQualityOperator(BaseOperator):
    """Custom Data Quality Operator."""
    
    template_fields = ('sql', 'expectation_suite')

    @apply_defaults
    def __init__(
        self: Self,
        validation_type: str = 'sql',
        conn_id: Optional[str] = None,
        sql: Optional[str] = None,
        pass_value: Any = None,
        ge_data_context_root: Optional[str] = None,
        expectation_suite: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        fail_on_warning: bool = True,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.validation_type = validation_type
        self.conn_id = conn_id
        self.sql = sql
        self.pass_value = pass_value
        self.ge_data_context_root = ge_data_context_root
        self.expectation_suite = expectation_suite
        self.data_asset_name = data_asset_name
        self.fail_on_warning = fail_on_warning

    def execute_sql_check(self: Self) -> bool:
        """Execute SQL-based data quality check."""
        hook = BaseHook.get_hook(self.conn_id)
        records = hook.get_records(self.sql)
        
        if not records:
            raise ValueError("Query returned no results")
            
        result = records[0][0]
        self.log.info(f"SQL check result: {result} (expected: {self.pass_value})")
        
        if result != self.pass_value:
            raise ValueError(
                f"Data quality check failed. "
                f"Got {result}, expected {self.pass_value}"
            )
        
        return True

    def execute_great_expectations(self: Self) -> ExpectationSuiteValidationResult:
        """Execute Great Expectations validation."""
        context = ge.data_context.DataContext(self.ge_data_context_root)
        
        # Get connection details
        conn = BaseHook.get_connection(self.conn_id)
        connection_string = (
            f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        )
        
        # Create validator
        datasource = context.datasources.get("postgres_datasource", {})
        batch_kwargs = {
            "datasource": datasource,
            "schema": "public",
            "table": self.data_asset_name.split('.')[-1],
            "data_asset_name": self.data_asset_name,
            "create_engine_kwargs": {"pool_pre_ping": True}
        }
        
        validator = context.get_validator(
            batch_kwargs=batch_kwargs,
            expectation_suite_name=self.expectation_suite,
            execution_engine="SqlAlchemyExecutionEngine",
            data_context=context,
            batch_parameters={"query": self.sql} if self.sql else None
        )
        
        # Run validation
        validation_result = validator.validate()
        
        # Process results
        self.log.info(f"GE Validation Results:\n{validation_result}")
        
        if not validation_result.success:
            raise ValueError("Great Expectations validation failed")
            
        if self.fail_on_warning and any(
            result["success"] is False 
            for result in validation_result.results
        ):
            raise ValueError("Great Expectations validation had warnings")
        
        return validation_result

    def execute(
        self: Self,
        context: dict
    ) -> bool | ExpectationSuiteValidationResult:
        """Execute the appropriate validation type."""
        if self.validation_type == 'sql':
            if not self.sql:
                raise ValueError("SQL query is required for SQL validation")
            return self.execute_sql_check()
        
        elif self.validation_type == 'great_expectations':
            if not all([
                self.ge_data_context_root,
                self.expectation_suite,
                self.data_asset_name
            ]):
                raise ValueError("Missing required GE parameters")
            return self.execute_great_expectations()
        
        else:
            raise ValueError(f"Unknown validation type: {self.validation_type}")


# Example DAG using custom operator
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'custom_data_quality_checks',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    # SQL-based data quality check
    check_null_values = CustomDataQualityOperator(
        task_id='check_null_values',
        validation_type='sql',
        conn_id='postgres_conn',
        sql="""
        SELECT COUNT(*) 
        FROM sales 
        WHERE customer_id IS NULL OR amount IS NULL
        """,
        pass_value=0
    )
    
    # Great Expectations data validation
    ge_validation = CustomDataQualityOperator(
        task_id='ge_validation',
        validation_type='great_expectations',
        conn_id='postgres_conn',
        ge_data_context_root='/data/great_expectations',
        expectation_suite='sales_suite',
        data_asset_name='public.sales'
    )
    
    check_null_values >> ge_validation
