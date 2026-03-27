from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def run_spark_transform():
    # 1. Initialize Spark with Postgres Connector
    spark = (
        SparkSession.builder.appName("NIO_Silver_Transform")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0")
        .get_all()
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
