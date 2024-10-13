#!/bin/bash

spark_version=$(curl -s https://downloads.apache.org/spark/ | grep -oP 'spark-3\.\d+\.\d+' | sort -V | head -1)
spark_version_number=$(echo $spark_version | sed "s/spark-//g")

# build docker image with image name local-base-hadoop:latest
docker build -t base-spark:latest --build-arg spark_version=$spark_version_number ./spark || exit 1
docker build -t base-jupyter:latest --build-arg spark_version=$spark_version_number ./jupyter || exit 1
docker build -t local-base-hadoop:latest ./hadoop/base || exit 1

# running image to container, -d to run it in daemon mode
docker-compose up -d --force-recreate --build --remove-orphans --scale spark-worker=2 || exit 1

docker image prune -af
docker volume prune -af
