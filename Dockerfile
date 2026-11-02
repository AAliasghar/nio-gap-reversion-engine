FROM apache/airflow:2.7.1-python3.10

USER root

# 1. Install Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Set Java Home (so PySpark can find it)
# ENV JAVA_HOME=/usr/lib/jvm/default-java
# ENV PATH=$PATH:$JAVA_HOME/bin

# 2. Set Java Home (Using the specific path for OpenJDK 11) (so PySpark can find it)
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Ensure the airflow user can access the Java binary
RUN chmod -R +rX /usr/lib/jvm/java-11-openjdk-amd64

USER airflow


# 3. Install Python libraries (including PySpark and dbt).
# Every version is pinned: unpinned packages made the image impossible to rebuild from
# scratch (pip got stuck resolving today's versions against Airflow's).
# PySpark 3.5 is needed for pandas 2: with 3.4.1, DataFrame.toPandas() fails on timestamps.
RUN pip install --no-cache-dir \
    yfinance==1.2.0 \
    pandas==2.1.0 \
    sqlalchemy==1.4.49 \
    psycopg2-binary==2.9.7 \
    scikit-learn==1.7.2 \
    pyspark==3.5.3 \
    dbt-postgres==1.7.0 \
    dbt-core==1.7.0

