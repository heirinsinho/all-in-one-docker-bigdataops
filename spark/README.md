# EXAMPLE OF SPARK SUBMIT THROUGH CONSOLE

spark-submit --master spark://spark-master:7077 --name example-spark --deploy-mode client
/opt/spark/examples/src/main/python/sql/basic.py