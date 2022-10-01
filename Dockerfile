FROM openjdk:8-slim

ARG SPARK_VERSION=3.3.0

RUN apt update; \
    apt upgrade -y; \
    apt install -y \
    python3-pip \
    wget \
    procps \
    curl

ENV SPARK_VERSION ${SPARK_VERSION}
ENV HADOOP_VERSION 3
ENV TAR_FILE spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
ENV SPARK_HOME /opt/spark
ENV PATH "${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin"
ENV HADOOP_HOME /opt/hadoop
ENV HADOOP_CONF_DIR ${HADOOP_HOME}/conf
ENV ICEBERG_VERSION=0.14.1

# download spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${TAR_FILE}

RUN tar -xzvf ${TAR_FILE} -C /opt; \
    ln -sL /opt/${TAR_FILE%.tgz} ${SPARK_HOME}; \
    rm /${TAR_FILE}

WORKDIR ${SPARK_HOME}

# extensions
RUN wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.3_2.12-${ICEBERG_VERSION}.jar -P ${SPARK_HOME}/jars

ADD entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]
