FROM apache/airflow:3.3.1

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl openjdk-17-jre-headless \
    && mkdir -p /opt/spark-extra-jars \
    && curl -fL -o /opt/spark-extra-jars/hadoop-aws-3.4.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
    && curl -fL -o /opt/spark-extra-jars/aws-sdk-bundle-2.24.6.jar https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY airflow/requirements-sanlam.txt /tmp/requirements-sanlam.txt
RUN pip install --no-cache-dir -r /tmp/requirements-sanlam.txt

# Put Hadoop S3A dependencies on PySpark's default classpath.  Keeping them in
# a separate directory is not sufficient when Spark starts inside Airflow.
USER root
RUN cp /opt/spark-extra-jars/hadoop-aws-3.4.1.jar \
       /home/airflow/.local/lib/python3.13/site-packages/pyspark/jars/ \
    && cp /opt/spark-extra-jars/aws-sdk-bundle-2.24.6.jar \
       /home/airflow/.local/lib/python3.13/site-packages/pyspark/jars/ \
    && chown airflow:root \
       /home/airflow/.local/lib/python3.13/site-packages/pyspark/jars/hadoop-aws-3.4.1.jar \
       /home/airflow/.local/lib/python3.13/site-packages/pyspark/jars/aws-sdk-bundle-2.24.6.jar

USER airflow
