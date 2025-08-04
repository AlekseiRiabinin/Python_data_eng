from typing import Self
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.window import Window


class SparkDataScience:
    """Common PySpark patterns for data science."""
    
    def __init__(self: Self, spark: SparkSession):
        self.spark = spark
    
    def calculate_rfm(
        self: Self, 
        transactions: DataFrame,
        customer_id_col: str = "customer_id",
        date_col: str = "transaction_date",
        amount_col: str = "amount"
    ) -> DataFrame:
        """Calculate RFM metrics (Recency, Frequency, Monetary) using distributed computing."""
        max_date = transactions.agg(F.max(date_col)).collect()[0][0]
        
        rfm = (transactions
            .groupBy(customer_id_col)
            .agg(
                F.datediff(F.lit(max_date), F.max(date_col)).alias("recency"),
                F.count("*").alias("frequency"),
                F.sum(amount_col).alias("monetary")
            ))
        
        window = Window.orderBy("recency")
        rfm = rfm.withColumn("r_score", F.ntile(5).over(window.asc()))
        
        window = Window.orderBy("frequency")
        rfm = rfm.withColumn("f_score", F.ntile(5).over(window.desc()))
        
        window = Window.orderBy("monetary")
        rfm = rfm.withColumn("m_score", F.ntile(5).over(window.desc()))
        
        rfm = rfm.withColumn(
            "rfm_segment",
            F.concat(F.col("r_score"), F.col("f_score"), F.col("m_score"))
        )
        
        return rfm
    
    def process_large_json(
        self: Self,
        path: str,
        schema: StructType,
        max_records_per_file: int = 100000
    ) -> DataFrame:
        """Process large JSON files with schema enforcement and size control."""
        df = self.spark.read.schema(schema).json(path)
        
        approx_count = df.rdd.countApprox(timeout=1000)
        partitions = max(1, int(approx_count / max_records_per_file))
        
        return df.repartition(partitions)
