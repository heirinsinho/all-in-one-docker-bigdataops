FROM apache/airflow:2.7.3-python3.11

COPY ./requirements.txt requirements.txt
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         gcc \
         heimdal-dev \
         procps \
         openjdk-11-jdk \
         ant \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
RUN export JAVA_HOME

USER airflow
RUN pip install -r requirements.txt

COPY dags/utils/utils.py utils.py

CMD ["webserver"]