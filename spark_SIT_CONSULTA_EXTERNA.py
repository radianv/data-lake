from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.functions import explode

if __name__ == "__main__":

  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("Data Frame Join").set("spark.app.id", "MongoSparkConnectorTour")
  sc = SparkContext(conf=conf)
  
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)

  sqlContext = HiveContext(sc)
  df = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource").load()
  raw_data=df.select(df["_id"].alias('id')\
  ,df["className"].alias('className')\
  ,df["fecCaptura"].alias('fecCaptura')\
  ,df["sdCapturista.refApellidoPaterno"].alias('refApellidoPaterno')\
  ,explode(df["sdEncabezado.sdespecialidad"]).alias("sdespecialidad"))\
  .select("id","className","fecCaptura","refApellidoPaterno","sdespecialidad.cveEspecialidad")
  raw_data.write.mode("append").save('/apps/hive/warehouse/mytable_parquet', format='parquet')
