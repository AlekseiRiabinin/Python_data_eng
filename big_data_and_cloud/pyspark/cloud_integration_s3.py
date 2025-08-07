from typing import Self
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.dataframe import DataFrame


class S3SparkIntegration:
    """AWS S3 integration for PySpark."""
    
    def __init__(self: Self, spark: SparkSession) -> None:
        self.spark = spark
    
    def configure_s3(
        self: Self,
        access_key: str,
        secret_key: str,
        endpoint: str = "s3.amazonaws.com"
    ) -> None:
        """Configure Hadoop for S3 access."""
        hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.access.key", access_key)
        hadoop_conf.set("fs.s3a.secret.key", secret_key)
        hadoop_conf.set("fs.s3a.endpoint", endpoint)
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    def stream_from_s3(
        self: Self,
        path: str,
        schema: StructType,
        checkpoint_location: str
    ) -> DataFrame:
        """Create streaming DataFrame from S3 bucket."""
        return (self.spark.readStream
            .schema(schema)
            .option("maxFilesPerTrigger", 100)
            .option("cloud.region", "eu-central-1")
            .option("checkpointLocation", checkpoint_location)
            .parquet(path))
