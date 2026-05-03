FROM apache/spark:3.5.1-java17

USER root

ARG DELTA_VERSION=3.2.0
ARG SCALA_VERSION=2.12
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.367

# Download Delta Lake JARs
RUN curl -fsSL https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
    -o /opt/spark/jars/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar && \
    curl -fsSL https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
    -o /opt/spark/jars/delta-storage-${DELTA_VERSION}.jar

# Download Hadoop AWS + AWS SDK for S3A filesystem
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

USER spark
