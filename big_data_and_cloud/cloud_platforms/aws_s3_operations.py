import psycopg2
from typing import Self, Any, Optional, Generator
from contextlib import contextmanager


class RedshiftClient:
    """Redshift data warehouse client."""
    
    def __init__(
        self: Self,
        host: str,
        user: str,
        password: str,
        database: str,
        port: int = 5439
    ) -> None:
        self.conn_str = (
            f"host={host} dbname={database} "
            f"user={user} password={password} port={port}"
        )

    @contextmanager
    def _get_cursor(self: Self) -> Generator:
        """Context manager for database connections."""
        conn = psycopg2.connect(self.conn_str)
        try:
            with conn.cursor() as cur:
                yield cur
                conn.commit()
        finally:
            conn.close()
    
    def execute_query(
        self: Self,
        query: str,
        params: Optional[tuple] = None
    ) -> list[dict[str, Any]]:
        """Execute a SQL query and return results as dictionaries."""
        with self._get_cursor() as cur:
            cur.execute(query, params or ())
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
            return []
    
    def copy_from_s3(
        self: Self,
        table: str,
        s3_path: str,
        iam_role: str,
        format_options: dict[str, str]
    ) -> int:
        """Load data from S3 to Redshift using COPY command."""
        format_clause = " ".join(f"{k} '{v}'" for k, v in format_options.items())
        query = f"""
        COPY {table}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        {format_clause}
        """

        with self._get_cursor() as cur:
            cur.execute(query)
            return cur.rowcount


# Usage Example
if __name__ == "__main__":
    redshift = RedshiftClient(
        host="my-cluster.123456.eu-central-1.redshift.amazonaws.com",
        user="admin",
        password="securepassword",
        database="analytics"
    )
    
    # Load data from S3
    rows_loaded = redshift.copy_from_s3(
        table="sales_transactions",
        s3_path="s3://my-data-lake/raw/sales/2023-10-01.csv",
        iam_role="arn:aws:iam::123456789012:role/RedshiftLoadRole",
        format_options={
            "FORMAT": "CSV",
            "DELIMITER": ",",
            "IGNOREHEADER": "1"
        }
    )
    print(f"Loaded {rows_loaded} rows into Redshift")
    
    # Query data
    results = redshift.execute_query("""
        SELECT product_id, SUM(amount) as total_sales
        FROM sales_transactions
        WHERE transaction_date >= '2023-01-01'
        GROUP BY product_id
        ORDER BY total_sales DESC
        LIMIT 10
    """)
    print("Top selling products:", results)
