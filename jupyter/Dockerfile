FROM jupyter/base-notebook:x86_64-python-3.11.6

USER root
RUN dpkg --configure -a && apt-get update && apt-get upgrade -y \
  && apt-get install -y --no-install-recommends curl gcc openjdk-11-jdk ncat

ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64/"

RUN mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

ARG spark_version
ENV SPARK_VERSION="spark-${spark_version}"
ENV SPARK_URL=https://downloads.apache.org/spark/${SPARK_VERSION}/${SPARK_VERSION}-bin-hadoop3.tgz
RUN curl $SPARK_URL -o ${SPARK_VERSION}-bin-hadoop3.tgz \
 && tar xvzf ${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf ${SPARK_VERSION}-bin-hadoop3.tgz

ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}

WORKDIR /home/jovyan
ARG spark_version
ENV PYSPARK_VERSION="pyspark==${spark_version}"

COPY ./requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
RUN pip install ${PYSPARK_VERSION}

ENV SCALA_VERSION=2.12
ENV POSTGRESQL_VERSION=42.7.4

RUN curl -fsSL -o ${SPARK_HOME}/jars/postgresql-${POSTGRESQL_VERSION}.jar \
        https://jdbc.postgresql.org/download/postgresql-${POSTGRESQL_VERSION}.jar && \
    curl -fsSL -o ${SPARK_HOME}/jars/spark-sql-kafka-0-10_${SCALA_VERSION}-${spark_version}.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${spark_version}/spark-sql-kafka-0-10_${SCALA_VERSION}-${spark_version}.jar && \
    curl -fsSL -o ${SPARK_HOME}/jars/kafka-clients-3.3.2.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar && \
    curl -fsSL -o ${SPARK_HOME}/jars/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${spark_version}.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${spark_version}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${spark_version}.jar && \
    curl -fsSL -o ${SPARK_HOME}/jars/commons-pool2-2.12.0.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar

ENTRYPOINT ["start-notebook.sh", "--NotebookApp.token=''"]