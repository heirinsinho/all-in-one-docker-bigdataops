FROM jupyter/base-notebook:x86_64-python-3.11.6

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends curl gcc openjdk-11-jdk

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

COPY ./jupyter/requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
RUN pip install ${PYSPARK_VERSION}

ENTRYPOINT ["start-notebook.sh", "--NotebookApp.token=''"]