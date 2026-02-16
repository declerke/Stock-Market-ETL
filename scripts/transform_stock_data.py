import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def main(project_id, credentials_path, bucket_name, mode):
    spark = SparkSession.builder \
        .appName(f"Stock-ETL-{mode}") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_path) \
        .getOrCreate()

    if mode == "fundamentals":
        df = spark.read.parquet(f"gs://{bucket_name}/raw/fundamentals/*.parquet")
        
        df_clean = df.withColumn(
            "calculated_total_debt", 
            F.coalesce(F.col("Short Term Debt"), F.lit(0)) + F.coalesce(F.col("Long Term Debt"), F.lit(0))
        ).withColumn(
            "net_margin", F.col("Net Income") / F.col("Revenue")
        ).withColumn(
            "current_ratio", F.col("Total Current Assets") / F.col("Total Current Liabilities")
        ).withColumn(
            "debt_to_equity", F.col("calculated_total_debt") / F.col("Total Equity")
        ).withColumnRenamed("calculated_total_debt", "Total Debt")
        
        df_clean.withColumn("year", F.year("Report Date")) \
                .write.mode("overwrite").partitionBy("year") \
                .parquet(f"gs://{bucket_name}/transformed/fundamentals/")

    elif mode == "prices":
        prices = spark.read.parquet(f"gs://{bucket_name}/raw/prices/*.parquet")
        
        window_spec = Window.partitionBy("Ticker").orderBy("Date")
        df_clean = prices.withColumn("sma_20", F.avg("Close").over(window_spec.rowsBetween(-19, 0))) \
                         .withColumn("daily_return", (F.col("Close") / F.lag("Close", 1).over(window_spec)) - 1)
        
        df_clean.withColumn("year", F.year("Date")).withColumn("month", F.month("Date")) \
                .write.mode("overwrite").partitionBy("year", "month") \
                .parquet(f"gs://{bucket_name}/transformed/prices/")

    spark.stop()

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])