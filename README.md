# Conección a MongoDB con Spark 

The official MongoDB Spark Connector.

[![Build Status](https://travis-ci.org/mongodb/mongo-spark.svg?branch=master)]
(https://travis-ci.org/mongodb/mongo-spark)  | [![Build Status](https://jenkins.10gen.com/job/mongo-spark/badge/icon)](https://jenkins.10gen.com/job/mongo-spark/)

## Guia para ejecutar el Ejemplo de Spark Submit

Los pasos que debemos ejecutar son:

- crear tabla de prueba en HIVE con parquet.
	```
	create external table mytable_parquet 
	(
	id struct <oid:string>,
	className string,
	fecCaptura timestamp,
	refApellidoPaterno ARRAY<STRING>,
	cveEspecialidad ARRAY<STRING>
	) Stored as parquet;
	```

- ejecutar el codigo pyspark [SPARK code](https://github.com/radianv/data-lake/blob/master/spark-code/spark_SIT_CONSULTA_EXTERNA.py).

- validar desde HIVE que la información cargo en HIVE.
	```
	select *  from mytable_parquet;	
	
	```






* radianv          radianstk@gmail.com

