FROM apache/airflow:2.8.1

USER root

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get install -y wget && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Download PostgreSQL JDBC driver
RUN mkdir -p /opt/airflow/jars && \
    curl -o /opt/airflow/jars/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -P /opt/spark-jars/

USER airflow

COPY requirements.txt /

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt" \
    -r /requirements.txt