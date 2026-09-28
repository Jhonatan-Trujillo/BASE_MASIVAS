# Conteo de palabras (RDD)

import os, sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

from pyspark.sql import SparkSession

# 1. CREAR la sesión de Spark
spark = SparkSession.builder \
    .appName("ConteoPalabras") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. LEER el texto: 1 elemento del RDD por cada línea
lineas = spark.sparkContext.textFile("noticias.txt")

# 3. TRANSFORMACIONES → solo construyen el plan (perezosas)

#  a) cada línea se parte en palabras
palabras = lineas.flatMap(lambda linea: linea.split(" "))
#  b) limpiar: minúsculas y sin puntuación
palabras = palabras.map(lambda p: p.lower().strip(".,;:!?¡¿"))
#  c) descartar vacíos y palabras de 1 letra
palabras = palabras.filter(lambda p: len(p) > 2)
#  d) cada palabra → par (palabra, 1)
pares    = palabras.map(lambda p: (p, 1))
#  e) sumar los 1 de cada palabra (aquí ocurre el SHUFFLE)
conteos  = pares.reduceByKey(lambda a, b: a + b)

# 4. ACCIONES → SOLO ahora se ejecuta todo el plan

for palabra, total in conteos.take(10):
    print(f"{palabra:<15} {total}")

print("Palabras únicas:", conteos.count())
spark.stop()