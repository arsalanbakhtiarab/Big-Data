from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Stram").getOrCreate()
lines = (spark.readStream.format("socket").option("host","localhost").option("port",4444).load())
words = lines.select(split(col("value"),"\\s").alias("word"))
counts = words.groupBy("word").count()
checkpointDir = r"C:/checkpoint/"
streamingQuery = (counts
                  .writeStream.format("console")
                  .outputMode("complete")
                  .trigger(processingTime="1 second")
                  .option("checkpointLocation",checkpointDir)
                  .start()
                 )
streamingQuery.awaitTermination()
