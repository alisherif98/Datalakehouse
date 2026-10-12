FROM flink:1.18.1-scala_2.12-java11
ARG ICEBERG_VERSION=1.5.2
ARG MVN=https://repo1.maven.org/maven2
WORKDIR /opt/flink/lib
ADD ${MVN}/org/apache/iceberg/iceberg-flink-runtime-1.18/${ICEBERG_VERSION}/iceberg-flink-runtime-1.18-${ICEBERG_VERSION}.jar .
ADD ${MVN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar .
ADD ${MVN}/org/apache/iceberg/iceberg-nessie/${ICEBERG_VERSION}/iceberg-nessie-${ICEBERG_VERSION}.jar .
ADD ${MVN}/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar .
ADD ${MVN}/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar .
# ADD from URL creates root-only files; let the flink user read them
RUN chmod 644 /opt/flink/lib/*.jar && chown flink:flink /opt/flink/lib/*.jar \
 && mkdir -p /opt/flink/plugins/s3-fs-hadoop \
 && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/
WORKDIR /opt/flink
