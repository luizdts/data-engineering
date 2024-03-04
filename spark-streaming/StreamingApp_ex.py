from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .appName("StructuredStreaming").getOrCreate()

arquivo = "./Json/2010-summary.json"
dir = "./results/landing"

df = spark.read.json(arquivo)
schema_df = df.schema

df.printSchema()

df_streaming = spark.readStream\
    .schema(schema_df)\
    .option("maxFilesPerTrigger", "1")\
    .json(dir)

resultado = df_streaming.groupBy("ORIGIN_COUNTRY_NAME").sum("count")

#.outputMode("complete")    
saida = resultado.writeStream\
    .outputMode("update")\
    .format("console")\
    .start()
