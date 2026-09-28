# DataFrames y Spark SQL


import os, sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. CREAR la sesión de Spark
spark = SparkSession.builder \
    .appName("VentasSQL") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. CREAR un DataFrame (tabla con columnas tipadas)
ventas = spark.createDataFrame([
    ("Ana",   "Bogotá",   120000),
    ("Luis",  "Medellín", 90000),
    ("Carla", "Bogotá",   150000),
], ["vendedor", "ciudad", "monto"])

# 3. FORMA A: API de DataFrames (Python)
ventas.groupBy("ciudad") \
      .agg(F.sum("monto").alias("total")) \
      .show()

# 4. FORMA B: SQL estándar (genera el MISMO plan)
ventas.createOrReplaceTempView("ventas")
spark.sql("""SELECT ciudad, SUM(monto) AS total
        FROM ventas GROUP BY ciudad
        ORDER BY total DESC""").show()

spark.stop()