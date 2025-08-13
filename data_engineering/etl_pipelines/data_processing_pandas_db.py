from datetime import datetime, timedelta
from typing import Any, Optional, Self, Callable
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow import DAG
from psycopg2.extras import execute_batch
import pandas as pd
import psycopg2


class CustomPostgresHook(BaseHook):
    """Custom PostgreSQL Hook."""
    
    def __init__(
        self: Self,
        postgres_conn_id: str = 'postgres_default'
    ) -> None:
        super().__init__()
        self.postgres_conn_id = postgres_conn_id
        self.conn = None

    def get_conn(self: Self) -> psycopg2.extensions.connection:
        """Returns a PostgreSQL connection object."""
        if self.conn is None or self.conn.closed:
            conn = self.get_connection(self.postgres_conn_id)
            self.conn = psycopg2.connect(
                host=conn.host,
                port=conn.port or 5432,
                user=conn.login,
                password=conn.password,
                dbname=conn.schema
            )
        return self.conn

    def get_pandas_df(
        self: Self,
        sql: str, parameters: Optional[dict] = None
    ) -> pd.DataFrame:
        """Executes SQL and returns a pandas DataFrame."""
        conn = self.get_conn()
        try:
            return pd.read_sql(sql, conn, params=parameters)
        finally:
            conn.close()

    def insert_rows(
        self: Self,
        table: str,
        rows: list[tuple] | pd.DataFrame,
        target_fields: Optional[list[str]] = None,
        commit_every: int = 1000,
        replace: bool = False
    ) -> None:
        """Insert rows into a table with batch support."""
        conn = self.get_conn()
        cursor = conn.cursor()

        try:
            if isinstance(rows, pd.DataFrame):
                rows = [tuple(x) for x in rows.to_records(index=False)]
            
            if not rows:
                self.log.info("No rows to insert")
                return
            
            columns = target_fields or []
            colnames = ",".join(columns)
            placeholder = ",".join(["%s"] * len(columns))
            
            action = "INSERT"
            if replace:
                action = "REPLACE"
            
            sql = f"{action} INTO {table} ({colnames}) VALUES ({placeholder})"
            
            execute_batch(cursor, sql, rows, page_size=commit_every)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()


class CustomPostgresOperator(BaseOperator):
    """Custom PostgreSQL Operator."""
    
    template_fields = ('sql', 'parameters')
    
    @apply_defaults
    def __init__(
        self: Self,
        sql: str | list[str],
        postgres_conn_id: str = 'postgres_default',
        parameters: Optional[dict | list[dict]] = None,
        autocommit: bool = False,
        handler: Optional[Callable] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.parameters = parameters
        self.autocommit = autocommit
        self.handler = handler
        
    def execute(self: Self, context: dict) -> Optional[Any]:
        """Execute operator."""
        hook = CustomPostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            if isinstance(self.sql, str):
                sqls = [self.sql]
                params = [self.parameters] if self.parameters else [None]
            else:
                sqls = self.sql
                params = self.parameters if self.parameters else [None] * len(sqls)
            
            results = []
            for sql, param in zip(sqls, params):
                self.log.info(f"Executing SQL: {sql}")
                cursor.execute(sql, param)
                if self.handler:
                    results.append(self.handler(cursor))
                
            if not self.autocommit:
                conn.commit()

            if results:
                return results[0] if len(results) == 1 else results
                
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()


# Example DAG using custom operators
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def process_user_data() -> None:
    """Process user data using custom hook."""
    hook = CustomPostgresHook(postgres_conn_id='postgres_conn')
    df = hook.get_pandas_df("SELECT * FROM raw_users")

    # Data transformation
    df['full_name'] = df['first_name'] + ' ' + df['last_name']
    df['created_at'] = pd.to_datetime(df['created_at'])

    # Save to processed table
    hook.insert_rows(
        table='processed_users',
        rows=df[['user_id', 'full_name', 'email', 'created_at']],
        target_fields=['user_id', 'full_name', 'email', 'created_at']
    )


with DAG(
    'custom_user_data_processing',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False
) as dag:
    
    create_table = CustomPostgresOperator(
        task_id='create_processed_table',
        postgres_conn_id='postgres_conn',
        sql="""
        CREATE TABLE IF NOT EXISTS processed_users (
            user_id INT PRIMARY KEY,
            full_name VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP
        );
        """
    )
    
    process_data = CustomPostgresOperator(
        task_id='process_user_data',
        python_callable=process_user_data
    )

    create_table >> process_data
