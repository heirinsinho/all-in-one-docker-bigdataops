MAKEFLAGS += --no-print-directory
MAKEFLAGS += --quiet

OS := $(shell uname -s || echo "MSYS")

# Check if we are on Windows (CMD or PowerShell)
ifneq ($(shell echo $(OS) | grep -E '^MSYS'), )
SPARK_CMD := powershell -NonInteractive -ExecutionPolicy Bypass -file ".\get_spark_version.ps1"
else
SPARK_CMD := curl -s https://downloads.apache.org/spark/ | grep -o 'spark-3\.[0-9]*\.[0-9]*' | sort -V | head -1 | sed "s/spark-//g"
endif

SPARK_VERSION := $(shell output=$$($(SPARK_CMD)); echo $${output:-"3.4.3"})

print-spark-version:
	@echo Detected OS: $(OS)
	@echo Target Spark Version: $(SPARK_VERSION)

build-spark:
	@echo "Building Spark Base..."
	make print-spark-version
	@docker build -t base-spark:latest --build-arg spark_version=$(SPARK_VERSION) ./spark &> /dev/null || exit 1

build-jupyter:
	@echo "Building Jupyter Base..."
	@docker build --platform linux/amd64 -t base-jupyter:latest --build-arg spark_version=$(SPARK_VERSION) ./jupyter &> /dev/null || exit 1

build-base-hadoop:
	@echo "Building Hadoop Base..."
	@docker build --platform linux/amd64 -t local-base-hadoop:latest ./hadoop/base &> /dev/null || exit 1

build-all: build-spark build-jupyter build-base-hadoop

ensure-spark-logs:
	@echo "Ensuring spark-logs dfs directory exists..."
	@sleep 30
	@docker exec namenode hdfs dfsadmin -safemode forceExit &> /dev/null
	@docker exec namenode hadoop fs -mkdir -p /shared/spark-logs

clean-up:
	@docker image prune -af &> /dev/null
	@docker volume prune -af &> /dev/null

start-spark:
	make build-spark
	make build-jupyter
	make build-base-hadoop
	@echo "Starting Spark and Jupyter services..."
	@docker-compose up -d --force-recreate --build --scale spark-worker=2 spark-master spark-worker jupyter namenode datanode1 datanode2 &> /dev/null || exit 1
	make clean-up

start-hadoop:
	make start-spark
	make build-base-hadoop
	@echo "Starting Hadoop services..."
	@docker-compose up -d --force-recreate --build namenode datanode1 datanode2 nodemanager resourcemanager historyserver spark-history-server &> /dev/null || exit 1
	make clean-up
	make ensure-spark-logs

start-kafka:
	@echo "Starting Kafka services..."
	@docker-compose up -d --force-recreate --build zookeeper kafka &> /dev/null || exit 1
	make clean-up

start-airflow:
	@echo "Starting Airflow service..."
	@docker-compose up -d --force-recreate --build airflow  &> /dev/null || exit 1
	make clean-up

start-hive:
	make start-hadoop
	@echo "Starting Hive and Hue services..."
	@docker-compose up -d --force-recreate --build postgres-hive metastore hiveserver2 huedb hue  &> /dev/null || exit 1
	make clean-up

start-all: start-hive start-airflow start-kafka