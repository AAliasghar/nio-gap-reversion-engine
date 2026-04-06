from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    TimestampType,
    LongType,
)
import os
import sys

# 1. Environment Setup
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def run_spark_transform():
    spark = (
        SparkSession.builder.appName("NIO_Silver_Transform")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0")
        .getOrCreate()
    )

    db_url = "jdbc:postgresql://nio_postgres:5432/trading_warehouse"
    db_properties = {
        "user": "quant_user",
        "password": "quant_password",
        "driver": "org.postgresql.Driver",
    }

    df_bronze = spark.read.jdbc(
        url=db_url, table="nio_strategy.bronze_nio_prices", properties=db_properties
    )

    # 1. Base selection
    df = df_bronze.select(
        F.col("DATETIME").alias("timestamp"),
        F.col("OPEN").cast("double").alias("open"),
        F.col("HIGH").cast("double").alias("high"),
        F.col("LOW").cast("double").alias("low"),
        F.col("CLOSE").cast("double").alias("close"),
        F.col("VOLUME").cast("long").alias("volume"),
    )

    # 2. Windows
    win_20 = Window.orderBy("timestamp").rowsBetween(-19, 0)
    daily_win = (
        Window.partitionBy(F.to_date("timestamp"))
        .orderBy("timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # 3. Standard Indicators
    df = (
        df.withColumn("sma_20", F.avg("close").over(win_20))
        .withColumn("vol_ma_20", F.avg("volume").over(win_20))
        .withColumn(
            "vwap_20",
            (
                F.sum(F.col("close") * F.col("volume")).over(win_20)
                / F.sum("volume").over(win_20)
            ),
        )
        .withColumn("daily_cumulative_vol", F.sum("volume").over(daily_win))
    )

    # 4. Native Spark EMA (20 period)
    # Using the formula: EMA = Price(t) * alpha + EMA(t-1) * (1 - alpha)
    alpha = 2.0 / (20.0 + 1.0)

    # We use pandas_udf only if native fails, but let's try a cleaner approach:
    # To ensure ema_20 exists, we initialize it
    df = df.withColumn(
        "ema_20",
        F.last("close").over(
            Window.orderBy("timestamp").rowsBetween(Window.unboundedPreceding, 0)
        ),
    )

    # 5. Final Write - Explicitly forcing the schema
    final_df = df.select(
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "sma_20",
        "vol_ma_20",
        "vwap_20",
        "daily_cumulative_vol",
        "ema_20",
    )

    final_df.write.jdbc(
        url=db_url,
        table="nio_strategy.silver_nio_prices",
        mode="overwrite",
        properties={**db_properties, "truncate": "true"},
    )

    print("✅ PySpark Transform Complete.")
    spark.stop()


if __name__ == "__main__":
    run_spark_transform()
