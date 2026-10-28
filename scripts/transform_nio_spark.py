from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    from config import get_db_properties, get_jdbc_url
except ImportError:  # pragma: no cover
    from scripts.config import get_db_properties, get_jdbc_url

import os
import sys

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def run_spark_transform():
    spark = (
        SparkSession.builder.appName("NIO_Silver_Transform")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0")
        .getOrCreate()
    )

    db_url = get_jdbc_url()
    db_properties = get_db_properties()

    try:
        df_bronze = spark.read.jdbc(
            url=db_url, table="nio_strategy.bronze_nio_prices", properties=db_properties
        )

        df = df_bronze.select(
            F.col("DATETIME").alias("timestamp"),
            F.col("OPEN").cast("double").alias("open"),
            F.col("HIGH").cast("double").alias("high"),
            F.col("LOW").cast("double").alias("low"),
            F.col("CLOSE").cast("double").alias("close"),
            F.col("VOLUME").cast("long").alias("volume"),
        )

        win_20 = Window.orderBy("timestamp").rowsBetween(-19, 0)
        daily_win = (
            Window.partitionBy(F.to_date("timestamp"))
            .orderBy("timestamp")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )

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

        pandas_df = df.toPandas().sort_values("timestamp")
        pandas_df["ema_20"] = pandas_df["close"].ewm(span=20, adjust=False).mean()
        df = spark.createDataFrame(pandas_df)

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
    finally:
        spark.stop()


if __name__ == "__main__":
    run_spark_transform()
