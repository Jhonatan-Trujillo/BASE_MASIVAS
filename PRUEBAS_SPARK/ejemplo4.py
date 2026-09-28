# Structured Streaming (tiempo real)

import os, sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Stream").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 1. FUENTE: leer un socket como una tabla "infinita"
flujo = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 2. TRANSFORMAR (igual que un DataFrame estático)
#    split: "hola mundo" → ["hola", "mundo"]
#    explode: 1 fila → 1 fila por palabra
palabras = flujo.select(
    F.explode(F.split(flujo.value, " ")).alias("palabra")
).groupBy("palabra").count()

# 3. INICIAR la consulta: imprime el conteo en cada micro-lote
consulta = palabras.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

consulta.awaitTermination()   # queda corriendo; Ctrl+C para salir