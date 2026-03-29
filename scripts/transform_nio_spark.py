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

    # 3. Read Raw "Bronze" Data
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

    # Define the Windows
    # Window 1: Last 20 candles (100 minutes)
    short_window = Window.orderBy("timestamp").rowsBetween(-19, 0)

    # Window 2: Last 1560 candles (Approximately 20 Trading Days)
    daily_window = Window.orderBy("timestamp").rowsBetween(-1559, 0)

    # 4.  Apply Transformations
    df_silver = (
        df.withColumn("sma_20_intraday", F.avg("close").over(short_window))
        .withColumn("vol_ma_20_intraday", F.avg("volume").over(short_window))
        .withColumn("sma_20_daily", F.avg("close").over(daily_window))
        .withColumn("vol_ma_20_daily", F.avg("volume").over(daily_window))
    )

    # 5. Write to "Silver" Table
    df_silver.write.jdbc(
        url=db_url,
        table="silver_nio_prices",
        mode="overwrite",
        properties=db_properties,
    )

    print("✅ PySpark Transform Complete: Silver Table Updated.")
    spark.stop()
