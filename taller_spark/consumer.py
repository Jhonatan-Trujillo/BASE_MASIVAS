import os
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, floor, expr, when, lit, concat_ws
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import json

# --- Parámetros ---
ALTITUD_MINIMA = 90.0
BATERIA_CRITICA = 10.0
VELOCIDAD_MAXIMA_DRON = 65.0
VELOCIDAD_ORBITAL_MINIMA = 27200.0
DISTANCIA_RIESGO_M = 300.0

TAMANIO_CELDA_GRADOS = 0.01
TAMANIO_CELDA_ALTITUD = 50.0

HORIZONTE_PROYECCION_SEG = 10  # segundos hacia adelante que proyectamos la trayectoria

RUTA_SALIDA_ESTADO = "estado_drones.json"
RUTA_SALIDA_COLISIONES = "colisiones.json"

MAX_HISTORIAL = 30

# 1) SESIÓN
spark = SparkSession.builder \
    .appName("GestionTraficoAeroespacial") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

# 2) CONTEXTO / LECTURA
esquema = StructType([
    StructField("id", StringType()),
    StructField("tipo", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("altitud", DoubleType()),
    StructField("velocidad", DoubleType()),
    StructField("rumbo", DoubleType()),
    StructField("bateria", DoubleType()),
    StructField("timestamp", StringType()),
])

lineas = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 3) TRANSFORMACIÓN
datos = lineas.select(from_json(col("value"), esquema).alias("v")).select("v.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
    .withWatermark("timestamp", "10 seconds")

# --- Condiciones individuales ---
datos_evaluados = datos \
    .withColumn(
        "alerta_bateria",
        when(col("bateria") <= BATERIA_CRITICA,
             concat_ws(" ", lit("ADVERTENCIA: batería crítica en"), col("id"),
                       lit("- cesar operaciones inmediatamente")))
    ) \
    .withColumn(
        "alerta_altitud",
        when((col("tipo") == "dron") & (col("altitud") < ALTITUD_MINIMA),
             concat_ws(" ", lit("ADVERTENCIA: altitud baja en"), col("id"),
                       lit("- ascienda para evitar colisión con obstáculos")))
    ) \
    .withColumn(
        "alerta_velocidad",
        when((col("tipo") == "dron") & (col("velocidad") > VELOCIDAD_MAXIMA_DRON),
             concat_ws(" ", lit("ADVERTENCIA: velocidad excesiva en"), col("id"),
                       lit("- reduzca velocidad")))
    ) \
    .withColumn(
        "alerta_orbita",
        when((col("tipo") == "satelite") & (col("velocidad") < VELOCIDAD_ORBITAL_MINIMA),
             concat_ws(" ", lit("ADVERTENCIA: velocidad orbital baja en"), col("id"),
                       lit("- riesgo de decaimiento orbital, corrija propulsión")))
    )

# --- Proyección de trayectoria: dónde estará cada vehículo dentro de HORIZONTE_PROYECCION_SEG ---

datos_proyectados = datos_evaluados \
    .withColumn(
        "distancia_proyectada_km",
        col("velocidad") * (lit(HORIZONTE_PROYECCION_SEG) / 3600.0)
    ) \
    .withColumn(
        "lat_proyectada",
        col("lat") + (col("distancia_proyectada_km") / 111.32) * expr("cos(radians(rumbo))")
    ) \
    .withColumn(
        "lon_proyectada",
        col("lon") + (col("distancia_proyectada_km") / 111.32) * expr("sin(radians(rumbo))")
    ) \
    .withColumn("celda_lat", floor(col("lat_proyectada") / TAMANIO_CELDA_GRADOS)) \
    .withColumn("celda_lon", floor(col("lon_proyectada") / TAMANIO_CELDA_GRADOS)) \
    .withColumn("celda_alt", floor(col("altitud") / TAMANIO_CELDA_ALTITUD)) \
    .withColumn("ventana", window(col("timestamp"), "5 seconds"))

# --- Ventana 3D + self-join, ahora sobre POSICIONES PROYECTADAS ---
a = datos_proyectados.alias("a")
b = datos_proyectados.alias("b")

condicion_join = expr("""
    a.ventana = b.ventana AND
    a.celda_lat = b.celda_lat AND
    a.celda_lon = b.celda_lon AND
    a.celda_alt = b.celda_alt AND
    a.id < b.id
""")

pares = a.join(b, condicion_join) \
    .withColumn(
        "distancia_m",
        expr("""
            sqrt(
                pow((a.lat_proyectada - b.lat_proyectada) * 111320, 2) +
                pow((a.lon_proyectada - b.lon_proyectada) * 111320 * cos(radians(a.lat_proyectada)), 2) +
                pow(a.altitud - b.altitud, 2)
            )
        """)
    ) \
    .filter(col("distancia_m") < DISTANCIA_RIESGO_M) \
    .withColumn("rumbo_corregido_b", (col("b.rumbo") + lit(15)) % 360) \
    .withColumn(
        "alerta_colision",
        concat_ws(" ", lit("RIESGO DE COLISIÓN PROYECTADO entre"), col("a.id"), lit("y"), col("b.id"),
                  lit("- en"), lit(HORIZONTE_PROYECCION_SEG), lit("s estarán a"),
                  col("distancia_m").cast("string"), lit("m"),
                  lit("- nuevo rumbo sugerido para"), col("b.id"), lit(":"),
                  col("rumbo_corregido_b").cast("string"))
    ) \
    .select("a.id", "b.id", "distancia_m", "rumbo_corregido_b", "alerta_colision")

# --- Detección de "alerta nueva" (evita repetir en consola) ---
estado_previo = {}

def es_alerta_nueva(id_vehiculo, campo, valor_actual):
    clave = (id_vehiculo, campo)
    activa_ahora = valor_actual is not None
    estaba_activa = estado_previo.get(clave, False)
    estado_previo[clave] = activa_ahora
    return activa_ahora and not estaba_activa

# --- Historial acumulado de eventos ---
historial_estado = []
contador_eventos_estado = 0

historial_colisiones = []
contador_eventos_colision = 0


def procesar_lote(df_estado, id_lote):
    global contador_eventos_estado

    estado = df_estado.select(
        "id", "tipo", "lat", "lon", "altitud", "velocidad", "rumbo", "bateria",
        "alerta_bateria", "alerta_altitud", "alerta_velocidad", "alerta_orbita"
    ).collect()

    registros = [row.asDict() for row in estado]

    for r in registros:
        r["alerta_bateria_nueva"] = es_alerta_nueva(r["id"], "bateria", r["alerta_bateria"])
        r["alerta_altitud_nueva"] = es_alerta_nueva(r["id"], "altitud", r["alerta_altitud"])
        r["alerta_velocidad_nueva"] = es_alerta_nueva(r["id"], "velocidad", r["alerta_velocidad"])
        r["alerta_orbita_nueva"] = es_alerta_nueva(r["id"], "orbita", r["alerta_orbita"])

        for campo, es_nueva, clase in (
            ("alerta_bateria", r["alerta_bateria_nueva"], "alerta-bateria"),
            ("alerta_altitud", r["alerta_altitud_nueva"], "alerta-altitud"),
            ("alerta_velocidad", r["alerta_velocidad_nueva"], "alerta-velocidad"),
            ("alerta_orbita", r["alerta_orbita_nueva"], "alerta-orbita"),
        ):
            if r.get(campo) and es_nueva:
                contador_eventos_estado += 1
                historial_estado.append({"id": contador_eventos_estado, "texto": r[campo], "clase": clase})
                print(f"[CONSUMIDOR] {r[campo]}")

    if len(historial_estado) > MAX_HISTORIAL:
        del historial_estado[: len(historial_estado) - MAX_HISTORIAL]

    with open(RUTA_SALIDA_ESTADO, "w", encoding="utf-8") as f:
        json.dump(
            {"lote": id_lote, "vehiculos": registros, "eventos": historial_estado},
            f, ensure_ascii=False, default=str
        )


def procesar_lote_colisiones(df_colisiones, id_lote):
    global contador_eventos_colision

    filas = df_colisiones.collect()

    for row in filas:
        contador_eventos_colision += 1
        historial_colisiones.append({
            "id": contador_eventos_colision,
            "texto": row["alerta_colision"],
            "clase": "alerta-colision"
        })
        print(f"[CONSUMIDOR] {row['alerta_colision']}")

    if len(historial_colisiones) > MAX_HISTORIAL:
        del historial_colisiones[: len(historial_colisiones) - MAX_HISTORIAL]

    with open(RUTA_SALIDA_COLISIONES, "w", encoding="utf-8") as f:
        json.dump({"lote": id_lote, "eventos": historial_colisiones}, f, ensure_ascii=False, default=str)


# 4) ACCIÓN
consulta_estado = datos_proyectados.writeStream \
    .outputMode("update") \
    .foreachBatch(procesar_lote) \
    .trigger(processingTime="2 seconds") \
    .start()

consulta_colisiones = pares.writeStream \
    .outputMode("update") \
    .foreachBatch(procesar_lote_colisiones) \
    .trigger(processingTime="2 seconds") \
    .start()

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n[CONSUMIDOR] Deteniendo streams...")
    consulta_estado.stop()
    consulta_colisiones.stop()
    spark.stop()
    print("[CONSUMIDOR] Detenido correctamente.")