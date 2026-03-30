from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
import sys

# Force the JAVA_HOME path inside the script (in case it's not picked up from the environment)
os.environ["JAVA_HOME"] = (
    "/usr/lib/jvm/java-11-openjdk-amd64"  # Match docker exec version
)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def run_spark_transform():
    # 1. Initialize Spark with Postgres Connector
    spark = (
        SparkSession.builder.appName("NIO_Silver_Transform")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )

    # 2. Connection Details
    db_url = "jdbc:postgresql://nio_postgres:5432/trading_warehouse"
    db_properties = {
        "user": "quant_user",
        "password": "quant_password",
        "driver": "org.postgresql.Driver",
    }

    # 3.  Read bronze 5‑min data from Postgres
    df = spark.read.jdbc(
        url=db_url, table="bronze_nio_prices", properties=db_properties
    )

    # Select and rename columns for consistency
    df = df.select(
        F.col("DATETIME").alias("timestamp"),
        F.col("OPEN").alias("open"),
        F.col("HIGH").alias("high"),
        F.col("LOW").alias("low"),
        F.col("CLOSE").alias("close"),
        F.col("VOLUME").alias("volume"),
        F.col("LOAD_TIME").alias("load_time"),
    )

    # ---- Step 5: Daily aggregation ----
    daily = (
        df.withColumn("trade_date", F.to_date("timestamp"))
        .groupBy("trade_date")
        .agg(
            F.first("open").alias("daily_open"),
            F.max("high").alias("daily_high"),
            F.min("low").alias("daily_low"),
            F.last("close").alias("daily_close"),
            F.sum("volume").alias("daily_volume"),
        )
    )

    # ---- Step 6: 20-day rolling averages on daily data ----
    daily_window = Window.orderBy("trade_date").rowsBetween(-19, 0)
    daily_ma = (
        daily.withColumn("sma_20_daily", F.avg("daily_close").over(daily_window))
        .withColumn("vol_ma_20_daily", F.avg("daily_volume").over(daily_window))
        .select("trade_date", "sma_20_daily", "vol_ma_20_daily")
    )

    # ---- Step 7: Join back to 5‑min data ----
    df_with_date = df.withColumn("trade_date", F.to_date("timestamp"))
    df_silver = df_with_date.join(daily_ma, on="trade_date", how="left").select(
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "sma_20_daily",
        "vol_ma_20_daily",
    )

    # # Define the Windows
    # # Window 1: Last 20 candles (100 minutes)
    # short_window = Window.orderBy("timestamp").rowsBetween(-19, 0)

    # # Window 2: Last 1560 candles (Approximately 20 Trading Days)
    # daily_window = Window.orderBy("timestamp").rowsBetween(-1559, 0)

    # # 4.  Apply Transformations
    # df_silver = (
    #     df.withColumn("sma_20_intraday", F.avg("close").over(short_window))
    #     .withColumn("vol_ma_20_intraday", F.avg("volume").over(short_window))
    #     .withColumn("sma_20_daily", F.avg("close").over(daily_window))
    #     .withColumn("vol_ma_20_daily", F.avg("volume").over(daily_window))
    # )

    # 5. Write to "Silver" Table
    df_silver.write.jdbc(
        url=db_url,
        table="silver_nio_prices",
        mode="overwrite",
        properties=db_properties,
    )

    print("✅ PySpark Transform Complete: Silver Table Updated.")
    spark.stop()
