use ${hiveconf:MY_SCHEMA};

add JAR hdfs://localhost:8020/tmp/brickhouse-0.6.0.jar;
CREATE TEMPORARY FUNCTION array_index AS 'brickhouse.udf.collect.ArrayIndexUDF';
CREATE TEMPORARY FUNCTION numeric_range AS 'brickhouse.udf.collect.NumericRange';

DROP TABLE IF EXISTS ${hiveconf:MY_SCHEMA}.tableau_datamart;

CREATE TABLE IF NOT EXISTS ${hiveconf:MY_SCHEMA}.tableau_datamart AS
SELECT * FROM ${hiveconf:MY_SCHEMA}.rvt_reporting;

