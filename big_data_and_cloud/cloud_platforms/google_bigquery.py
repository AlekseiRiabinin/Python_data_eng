from google.cloud import bigquery
from typing import Self, Optional, Any


class BigQueryClient:
    """Google BigQuery client with type hints."""
    
    def __init__(self: Self, project_id: str) -> None:
        self.client = bigquery.Client(project=project_id)

    def run_query(
        self: Self,
        query: str,
        params: Optional[list[bigquery.ScalarQueryParameter]] = None
    ) -> list[dict[str, Any]]:
        """Execute a SQL query and return results as dictionaries."""
        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = params
        
        query_job = self.client.query(query, job_config=job_config)
        return [dict(row) for row in query_job.result()]
    
    def load_from_gcs(
        self: Self,
        table_id: str,
        gcs_uri: str,
        schema: list[dict[str, str]],
        format_options: dict[str, str]
    ) -> int:
        """Load data from Google Cloud Storage to BigQuery."""
        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField(f["name"], f["type"]) for f in schema],
            **format_options
        )
        
        load_job = self.client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )
        load_job.result()  # Wait for completion
        return load_job.output_rows


# Usage Example
if __name__ == "__main__":
    bq = BigQueryClient(project_id="my-gcp-project")
    
    # Load data from GCS
    rows_loaded = bq.load_from_gcs(
        table_id="my-gcp-project.analytics.sales",
        gcs_uri="gs://my-data-lake/raw/sales/*.parquet",
        schema=[
            {"name": "transaction_id", "type": "STRING"},
            {"name": "amount", "type": "FLOAT"},
            {"name": "transaction_date", "type": "DATE"}
        ],
        format_options={
            "source_format": bigquery.SourceFormat.PARQUET,
            "write_disposition": bigquery.WriteDisposition.WRITE_TRUNCATE
        }
    )
    print(f"Loaded {rows_loaded} rows into BigQuery")
    
    # Query data
    results = bq.run_query("""
        SELECT 
            EXTRACT(MONTH FROM transaction_date) as month,
            ROUND(SUM(amount), 2) as monthly_sales
        FROM `my-gcp-project.analytics.sales`
        WHERE EXTRACT(YEAR FROM transaction_date) = 2023
        GROUP BY month
        ORDER BY month
    """)
    print("Monthly sales:", results)
