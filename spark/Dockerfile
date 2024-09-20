FROM python:3.11.6-bullseye

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      rsync \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64/"

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

ARG spark_version
ENV SPARK_VERSION="spark-${spark_version}"
ENV SPARK_URL=https://downloads.apache.org/spark/${SPARK_VERSION}/${SPARK_VERSION}-bin-hadoop3.tgz
RUN curl $SPARK_URL -o ${SPARK_VERSION}-bin-hadoop3.tgz \
 && tar xvzf ${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf ${SPARK_VERSION}-bin-hadoop3.tgz

COPY ./spark/requirements.txt ./requirements.txt
RUN pip3 install -r requirements.txt

ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST="spark-master"
ENV SPARK_MASTER_PORT="7077"
ENV PYSPARK_PYTHON="python3"
ENV SPARK_LOG_DIR="/opt/spark/logs"
ENV SPARK_MASTER_LOG="/opt/spark/logs/spark-master.out"
ENV SPARK_WORKER_LOG="/opt/spark/logs/spark-worker.out"

USER root

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

COPY ./spark/entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

RUN mkdir -p $SPARK_LOG_DIR && \
touch $SPARK_MASTER_LOG && \
touch $SPARK_WORKER_LOG && \
ln -sf /dev/stdout $SPARK_MASTER_LOG && \
ln -sf /dev/stdout $SPARK_WORKER_LOG

CMD ["tail", "-f", "/dev/null"]