FROM apache/spark:3.5.3

USER root
# Note: Paths in Apache image are /opt/spark/ instead of /opt/bitnami/spark/
RUN apt-get update && apt-get install -y wget && \
    rm -rf /var/lib/apt/lists/*

RUN wget -P /opt/spark/jars/ \
    https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

COPY spark/transform_stock_data.py /opt/spark-apps/transform_stock_data.py

RUN chmod -R 777 /opt/spark/jars && \
    chmod -R 777 /opt/spark-apps

USER 1001
WORKDIR /opt/spark-apps
