# RDD — Resilient Distributed Dataset

import os, sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

# Requisitos: pip install pyspark  +  Java 8/11/17 en el equipo
from pyspark.sql import SparkSession   # API moderna (PySpark 3 y 4)

# 1. CREAR la sesión: "local[*]" = todos los núcleos de tu PC
spark = SparkSession.builder \
    .appName("mi-primera-app") \
    .master("local[*]") \
    .getOrCreate()

# Bajar el nivel de log para que no inunde la consola
spark.sparkContext.setLogLevel("ERROR")

# 2. LEER el archivo: un RDD donde cada elemento es una línea
rdd = spark.sparkContext.textFile("noticias.txt")

# 3. TRANSFORMACIÓN (perezosa → NO se ejecuta todavía)
solo_python = rdd.filter(lambda l: "python" in l.lower())

# 4. ACCIÓN → aquí sí se ejecuta todo el plan
conteo = solo_python.count()

print("Líneas que mencionan 'python':", conteo)
spark.stop()