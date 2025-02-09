MAKEFLAGS += --no-print-directory
MAKEFLAGS += --quiet

##OS := $(shell uname -s || echo $(OS))
#OS_LOWER := $(shell echo $(OS) | tr 'A-Z' 'a-z' | grep -i windows)
## Check if we are on Windows (CMD or PowerShell)
#ifneq ($(strip $(OS_LOWER)),)
##SPARK_VERSION := -$(shell powershell -NonInteractive -ExecutionPolicy Bypass -file ".\get_spark_version.ps1") || $(shell echo "3.4.4")
#else
##SPARK_VERSION := $(shell curl -s https://downloads.apache.org/spark/ | grep -o 'spark-3\.[0-9]*\.[0-9]*' | sort -V | head -1 | sed "s/spark-//g")
#endif
##
## If the output is empty, default to "3.4.4".
#ifeq ($(strip $(SPARK_VERSION)),)
#  SPARK_VERSION = 3.4.4
#endif

# Above code for extracting spark version is unstable
SPARK_VERSION = 3.4.4

print-spark-version:
	echo Target Spark Version: $(SPARK_VERSION)

build-spark:
	echo "Building Spark Base..."
	make print-spark-version
	docker build -t base-spark:latest --build-arg spark_version=$(SPARK_VERSION) ./spark > build.log 2>&1 || exit 1

build-jupyter:
	@echo "Building Jupyter Base..."
	@docker build --platform linux/amd64 -t base-jupyter:latest --build-arg spark_version=$(SPARK_VERSION) ./jupyter > build.log 2>&1 || exit 1

build-base-hadoop:
	@echo "Building Hadoop Base..."
	@docker build --platform linux/amd64 -t local-base-hadoop:latest ./hadoop/base > build.log 2>&1 || exit 1

build-airflow:
	@echo "Building Airflow Base..."
	@docker build --platform linux/amd64 -t base-airflow:latest --build-arg spark_version=$(SPARK_VERSION) ./airflow > build.log 2>&1 || exit 1


build-all: build-spark build-jupyter build-base-hadoop build-airflow

ensure-spark-logs:
	@echo "Ensuring spark-logs dfs directory exists..."
	@sleep 30
	@docker exec namenode hdfs dfsadmin -safemode forceExit
	@docker exec namenode hadoop fs -mkdir -p /shared/spark-logs

clean-up:
	@docker image prune -f > build.log 2>&1
	@docker volume prune -f > build.log 2>&1

start-spark:
	make build-spark
	make build-jupyter
	make build-base-hadoop
	@echo "Starting Spark and Jupyter services..."
	@docker-compose up -d --force-recreate --build --scale spark-worker=2 spark-master spark-worker jupyter namenode datanode1 datanode2 spark-history-server > build.log 2>&1 || exit 1
	make clean-up

start-hadoop:
	make start-spark
	make build-base-hadoop
	@echo "Starting Hadoop services..."
	@docker-compose up -d --force-recreate --build namenode datanode1 datanode2 nodemanager resourcemanager historyserver > build.log 2>&1 || exit 1
	make clean-up
	make ensure-spark-logs

start-kafka:
	@echo "Starting Kafka services..."
	@docker-compose up -d --force-recreate --build zookeeper kafka > build.log 2>&1 || exit 1
	make clean-up

start-airflow:
	@docker-compose up -d --force-recreate --build postgres > build.log 2>&1 || exit 1
	@sleep 15
	make build-airflow
	@echo "Starting Airflow service..."
	@docker-compose up -d --force-recreate --build airflow > build.log 2>&1 || exit 1
	make clean-up

start-hive:
	make start-hadoop
	@echo "Starting Hive and Hue services..."
	@docker-compose up -d --force-recreate --build postgres > build.log 2>&1 || exit 1
	@sleep 15
	@docker-compose up -d --force-recreate --build metastore hiveserver2 hue > build.log 2>&1 || exit 1
	make clean-up

start-streaming: start-spark start-kafka
start-all: start-hive start-airflow start-kafka