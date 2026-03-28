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


# # 3. Install Python libraries (including PySpark and DBT)
    RUN pip install --no-cache-dir \
    yfinance \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    scikit-learn \
    pyspark==3.4.1 \
    dbt-postgres==1.7.0 \
    dbt-core==1.7.0 

