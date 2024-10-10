OS := $(shell uname -s || echo "MSYS")

# Check if we are on Windows (CMD or PowerShell)
ifneq ($(shell echo $(OS) | grep -E '^MSYS'), )
SPARK_CMD := powershell -NonInteractive -ExecutionPolicy Bypass -file ".\get_spark_version.ps1"
else
SHELL := "/bin/sh"
SPARK_CMD := curl -s https://downloads.apache.org/spark/ | grep -o 'spark-3\.[0-9]*\.[0-9]*' | sort -V | head -1 | sed "s/spark-//g"
endif

SPARK_VERSION := $(shell $(SPARK_CMD))

print-spark-version:
	@echo $(SHELL)
	@echo Detected OS: $(OS)
	@echo Target Spark Version: $(SPARK_VERSION)

build-spark: print-spark-version
	@docker build -t base-spark:latest --build-arg spark_version=$(SPARK_VERSION) ./spark || exit 1

build-jupyter: print-spark-version
	@docker build --platform linux/amd64 -t base-jupyter:latest --build-arg spark_version=$(SPARK_VERSION) ./jupyter || exit 1

build-base-hadoop:
	@docker build --platform linux/amd64 -t base-hadoop:latest ./hadoop/base || exit 1

build-all: build-spark build-jupyter build-base-hadoop

ensure-spark-logs:
	@sleep 35
	@docker exec namenode hdfs dfsadmin -safemode forceExit
	@docker exec namenode hadoop fs -mkdir -p /shared/spark-logs

clean-up:
	@docker image prune -af
	@docker volume prune -af

start-jupyter:
	make build-jupyter
	@docker-compose up -d --force-recreate --build --remove-orphans --scale spark-worker=2 || exit 1
	make clean-up

start-all:
	make build-all
	@docker-compose up -d --force-recreate --build --remove-orphans --scale spark-worker=2 || exit 1
	make clean-up
	make ensure-spark-logs