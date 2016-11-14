use ${hiveconf:MY_SCHEMA};

DROP TABLE IF EXISTS ${hiveconf:MY_SCHEMA}.tableau_datamart;

CREATE TABLE IF NOT EXISTS ${hiveconf:MY_SCHEMA}.tableau_datamart AS
SELECT * FROM ${hiveconf:MY_SCHEMA}.rvt_reporting;

