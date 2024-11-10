CREATE DATABASE uax;
USE uax;
SET mapreduce.input.fileinputformat.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;
CREATE EXTERNAL TABLE IF NOT EXISTS uax.word_count (
    word STRING,
    counter INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/data/output/word_count';

SELECT *
FROM uax.word_count
ORDER BY counter desc
LIMIT 5;