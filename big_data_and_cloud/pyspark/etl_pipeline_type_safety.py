from typing import Any, Self
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,StructField, StringType,
    IntegerType, DateType, DoubleType
)
from pyspark.sql.functions import col
from pyspark.sql.dataframe import DataFrame


SCHEMA_CUSTOMERS = StructType([
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("signup_date", DateType(), nullable=True),
    StructField("lifetime_value", DoubleType(), nullable=True)
])

SCHEMA_ORDERS = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("order_date", DateType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("items", IntegerType(), nullable=True)
])


class PySparkETL:
    """Distributed ETL pipeline using PySpark."""
    
    def __init__(self: Self) -> None:
        self.spark = (SparkSession.builder
            .appName("CustomerAnalytics")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate())
        
        self._register_udfs()
    
    def _register_udfs(self: Self) -> None:
        """Register User Defined Functions."""
        def categorize_value(amount: float) -> str:
            if amount > 1000: return "high"
            elif amount > 100: return "medium"
            return "low"
        
        self.spark.udf.register("value_category", categorize_value)
    
    def extract_data(
        self: Self,
        customer_path: str,
        orders_path: str
    ) -> dict[str, DataFrame]:
        """Load data from storage with schema enforcement."""
        return {
            "customers": self.spark.read.schema(SCHEMA_CUSTOMERS).parquet(customer_path),
            "orders": self.spark.read.schema(SCHEMA_ORDERS).csv(orders_path, header=True)
        }
    
    def transform_data(
        self: Self,
        customers: DataFrame,
        orders: DataFrame
    ) -> DataFrame:
        """Join and aggregate data with distributed operations."""
        joined = customers.join(orders, "customer_id", "left")
        
        result = (joined
            .groupBy("customer_id", "name")
            .agg({"amount": "sum", "order_id": "count", "lifetime_value": "max"})
            .withColumnRenamed("sum(amount)", "total_spent")
            .withColumnRenamed("count(order_id)", "order_count")
            .withColumnRenamed("max(lifetime_value)", "lifetime_value")
            .withColumn("value_category", col("value_category(total_spent)"))
            .filter(col("order_count") > 0))
        
        return result
    
    def load_data(
        self: Self,
        df: DataFrame,
        output_path: str,
        mode: str = "overwrite"
    ) -> None:
        """Write results to storage with partitioning."""
        (df.write
            .partitionBy("value_category")
            .mode(mode)
            .parquet(output_path))
    
    def run_pipeline(
        self: Self,
        input_paths: dict[str, str],
        output_path: str
    ) -> dict[str, Any]:
        """Execute full ETL pipeline with logging."""
        metrics = {
            "start_time": datetime.isoformat(),
            "steps": {}
        }
        
        # Extraction
        data = self.extract_data(
            input_paths["customers"],
            input_paths["orders"]
        )
        metrics["steps"]["extract"] = {
            "customer_count": data["customers"].count(),
            "order_count": data["orders"].count()
        }
        
        # Transformation
        result = self.transform_data(
            data["customers"],
            data["orders"]
        )
        metrics["steps"]["transform"] = {
            "result_count": result.count()
        }
        
        # Loading
        self.load_data(result, output_path)
        metrics["steps"]["load"] = {
            "output_path": output_path,
            "partitions": result.rdd.getNumPartitions()
        }
        
        metrics["end_time"] = datetime.isoformat()
        return metrics


# Example Usage:
if __name__ == "__main__":
    etl = PySparkETL()
    
    metrics = etl.run_pipeline(
        input_paths={
            "customers": "s3a://data-lake/customers/",
            "orders": "s3a://data-lake/orders/*.csv"
        },
        output_path="s3a://data-warehouse/customer_metrics/"
    )
    
    print("ETL Metrics:")
    for step, stats in metrics["steps"].items():
        print(f"{step.upper()}: {stats}")
