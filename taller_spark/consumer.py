import os
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

import threading
from datetime import datetime, timezone

import pyspark
from pymongo import MongoClient, UpdateOne
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, floor, expr, when, lit, concat_ws
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# --- Parámetros del ejercicio ---
ALTITUD_MINIMA = 90.0
BATERIA_CRITICA = 10.0
VELOCIDAD_MAXIMA_DRON = 65.0
VELOCIDAD_ORBITAL_MINIMA = 27200.0
DISTANCIA_RIESGO_M = 300.0

TAMANIO_CELDA_GRADOS = 0.01
TAMANIO_CELDA_ALTITUD = 50.0

HORIZONTE_PROYECCION_SEG = 10

# --- Kafka ---
KAFKA_SERVIDOR = "localhost:9092"
TOPICOS = "telemetria-drones,telemetria-satelites"   # lo que publican productor1 y productor2
# El conector Kafka de Spark debe tener la misma versión que el PySpark instalado
PAQUETE_KAFKA = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"

# --- Lotes de alertas ---
TAMANIO_LOTE = 10   # cada cuántas alertas se hace el insert a la base de datos

# --- MongoDB ---
MONGO_URI = "mongodb://localhost:27017"
NOMBRE_BD = "trafico_aeroespacial"
LIMPIAR_AL_INICIAR = True      # borra vehículos y eventos en vivo de la corrida anterior
TTL_TELEMETRIA_SEG = 3600      # la telemetría histórica se elimina sola después de 1 hora

# 0) CONEXIÓN A MONGODB (si no está encendido, falla rápido con un mensaje claro)
cliente_mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
try:
    cliente_mongo.admin.command("ping")
except Exception as e:
    raise SystemExit(f"[CONSUMIDOR] No se pudo conectar a MongoDB en {MONGO_URI}: {e}")

bd = cliente_mongo[NOMBRE_BD]
col_vehiculos = bd["vehiculos"]      # estado actual (un documento por vehículo)
col_telemetria = bd["telemetria"]    # historial de lecturas
col_eventos = bd["eventos"]          # alertas EN VIVO (las lee el panel de la página)
col_alertas = bd["alertas"]          # histórico de alertas, insertado POR LOTES

if LIMPIAR_AL_INICIAR:
    col_vehiculos.delete_many({})
    col_eventos.delete_many({})      # el histórico "alertas" nunca se borra

col_telemetria.create_index("fecha", expireAfterSeconds=TTL_TELEMETRIA_SEG)
col_telemetria.create_index([("id", 1), ("fecha", -1)])
col_eventos.create_index([("ts", -1)])
col_alertas.create_index([("lote", 1)])

# La numeración de lotes continúa donde quedó la corrida anterior
ultimo_lote = col_alertas.find_one(sort=[("lote", -1)])
contador_lotes = ultimo_lote["lote"] if ultimo_lote else 0

# 1) SESIÓN (con el conector de Kafka; la primera vez lo descarga, así que tarda un poco)
spark = SparkSession.builder \
    .appName("GestionTraficoAeroespacial") \
    .master("local[*]") \
    .config("spark.jars.packages", PAQUETE_KAFKA) \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

# 2) CONTEXTO / LECTURA desde Kafka (suscrito a los dos tópicos a la vez)
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

mensajes = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVIDOR) \
    .option("subscribe", TOPICOS) \
    .option("startingOffsets", "latest") \
    .load()

# 3) TRANSFORMACIÓN (en Kafka el contenido llega como bytes: primero se convierte a texto)
datos = mensajes.select(from_json(col("value").cast("string"), esquema).alias("v")).select("v.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
    .withWatermark("timestamp", "10 seconds")

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

# Proyección de trayectoria (dead-reckoning a HORIZONTE_PROYECCION_SEG segundos)
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

# Ventana 3D + self-join sobre posiciones proyectadas
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
    .select(
        col("a.id").alias("id_a"),
        col("b.id").alias("id_b"),
        "distancia_m", "rumbo_corregido_b", "alerta_colision"
    )

# --- Memoria del driver: detecta cuándo una alerta APARECE (evita repetirla en cada lote) ---
estado_previo = {}

def es_alerta_nueva(id_vehiculo, campo, valor_actual):
    clave = (id_vehiculo, campo)
    activa_ahora = valor_actual is not None
    estaba_activa = estado_previo.get(clave, False)
    estado_previo[clave] = activa_ahora
    return activa_ahora and not estaba_activa

pares_activos_previos = set()

ALERTAS_INDIVIDUALES = (
    ("alerta_bateria", "bateria", "alerta-bateria"),
    ("alerta_altitud", "altitud", "alerta-altitud"),
    ("alerta_velocidad", "velocidad", "alerta-velocidad"),
    ("alerta_orbita", "orbita", "alerta-orbita"),
)


# --- LOTES: las alertas se acumulan en un buffer y se insertan de TAMANIO_LOTE en TAMANIO_LOTE ---
# Las dos queries de Spark corren en hilos distintos y ambas generan alertas, por eso el candado.
lock_lote = threading.Lock()
buffer_alertas = []


def guardar_lote(lote, parcial=False):
    """Inserta un lote de alertas en MongoDB con UN solo insert_many.
    Es el único punto que habla con la base de datos de los lotes: aquí se cambiaría
    el destino (por ejemplo, a SQLite o MySQL)."""
    global contador_lotes
    contador_lotes += 1
    ahora = datetime.now(timezone.utc)
    documentos = [
        dict(alerta, lote=contador_lotes, lote_parcial=parcial, guardado_en=ahora)
        for alerta in lote
    ]
    col_alertas.insert_many(documentos)
    print(f"[LOTE {contador_lotes}] {len(documentos)} alertas insertadas en MongoDB (colección 'alertas')")


def registrar_alertas(alertas):
    """Agrega alertas al buffer; cada vez que se junta un lote completo, lo inserta y lo vacía."""
    with lock_lote:
        for alerta in alertas:
            buffer_alertas.append(alerta)
            print(f"[BUFFER] {len(buffer_alertas)}/{TAMANIO_LOTE} alertas en el lote actual")
            if len(buffer_alertas) >= TAMANIO_LOTE:
                guardar_lote(list(buffer_alertas))
                buffer_alertas.clear()


# 4) ACCIÓN — foreachBatch: cada micro-lote de Spark se guarda en MongoDB
def procesar_lote(df_estado, id_lote):
    filas = df_estado.select(
        "timestamp", "id", "tipo", "lat", "lon", "altitud", "velocidad", "rumbo", "bateria",
        "alerta_bateria", "alerta_altitud", "alerta_velocidad", "alerta_orbita"
    ).collect()

    if not filas:
        return

    # En orden cronológico, para que "último estado" y "alerta nueva" sean correctos
    registros = sorted((row.asDict() for row in filas), key=lambda r: r["timestamp"])
    ahora = datetime.now(timezone.utc)

    upserts, telemetria, nuevas_alertas = [], [], []

    for r in registros:
        for campo, clave, clase in ALERTAS_INDIVIDUALES:
            if es_alerta_nueva(r["id"], clave, r[campo]):
                nuevas_alertas.append({"vehiculo": r["id"], "texto": r[campo], "clase": clase, "ts": ahora})
                print(f"[CONSUMIDOR] {r[campo]}")

        doc = {k: v for k, v in r.items() if k != "timestamp"}
        doc["fecha"] = ahora
        upserts.append(UpdateOne({"_id": r["id"]}, {"$set": doc}, upsert=True))
        telemetria.append(dict(doc))

    col_vehiculos.bulk_write(upserts)       # estado actual: un documento por vehículo
    col_telemetria.insert_many(telemetria)  # historial completo de lecturas

    if nuevas_alertas:
        col_eventos.insert_many([dict(a) for a in nuevas_alertas])  # panel en vivo (al instante)
        registrar_alertas(nuevas_alertas)                           # histórico (por lotes)


def procesar_lote_colisiones(df_colisiones, id_lote):
    global pares_activos_previos

    filas = df_colisiones.collect()

    # Un mismo par puede aparecer varias veces en el lote: nos quedamos con una fila por par
    pares_ahora = {}
    for row in filas:
        pares_ahora[tuple(sorted((row["id_a"], row["id_b"])))] = row

    ahora = datetime.now(timezone.utc)
    nuevas_alertas = []
    for par, row in pares_ahora.items():
        if par not in pares_activos_previos:   # solo avisa cuando el riesgo APARECE
            nuevas_alertas.append({
                "vehiculo": " y ".join(par),
                "texto": row["alerta_colision"],
                "clase": "alerta-colision",
                "ts": ahora,
            })
            print(f"[CONSUMIDOR] {row['alerta_colision']}")

    pares_activos_previos = set(pares_ahora.keys())

    if nuevas_alertas:
        col_eventos.insert_many([dict(a) for a in nuevas_alertas])
        registrar_alertas(nuevas_alertas)


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
    # Las alertas que quedaron en el buffer sin completar el lote se guardan como lote parcial
    with lock_lote:
        if buffer_alertas:
            guardar_lote(list(buffer_alertas), parcial=True)
            buffer_alertas.clear()
    cliente_mongo.close()
    print("[CONSUMIDOR] Detenido correctamente.")