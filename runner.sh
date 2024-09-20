#!/bin/bash

spark_version=$(curl -s https://downloads.apache.org/spark/ | grep -oP 'spark-3\.\d+\.\d+' | sort -V | head -1)
spark_version_number=$(echo $spark_version | sed "s/spark-//g")

# build docker image with image name base-hadoop:latest
docker build -t base-spark:latest -f ./spark/Dockerfile --build-arg spark_version=$spark_version_number . || exit 1
docker build -t base-jupyter:latest -f ./jupyter/Dockerfile --build-arg spark_version=$spark_version_number . || exit 1
docker build -t base-hadoop:latest -f ./hadoop/base/Dockerfile . || exit 1

# running image to container, -d to run it in daemon mode
docker-compose up -d --force-recreate --build --remove-orphans --scale spark-worker=2 || exit 1

docker image prune -af
docker volume prune -af
