import json
import os
import time

from flask import Flask, Response, jsonify, send_from_directory, stream_with_context
from kafka import KafkaConsumer, TopicPartition
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
NOMBRE_BD = "trafico_aeroespacial"
KAFKA_SERVIDOR = "localhost:9092"
TOPICOS_KAFKA = ["telemetria-drones", "telemetria-satelites"]   # los tópicos de productor1 y productor2

MAX_EVENTOS = 30             # cuántos eventos recientes se envían al mapa
INTERVALO_REVISION_SEG = 1   # cada cuánto revisa MongoDB si hay cambios para empujar

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__)
bd = MongoClient(MONGO_URI)[NOMBRE_BD]


def leer_estado():
    """Arma el paquete que ve el mapa: estado actual de la flota + últimos eventos."""
    vehiculos = list(bd["vehiculos"].find({}, {"_id": 0, "fecha": 0}).sort("id", 1))

    eventos = list(bd["eventos"].find().sort([("ts", -1), ("_id", -1)]).limit(MAX_EVENTOS))
    eventos.reverse()  # del más antiguo al más reciente

    return {
        "vehiculos": vehiculos,
        "eventos": [{"id": str(e["_id"]), "texto": e["texto"], "clase": e["clase"]} for e in eventos],
    }


def estado_destino():
    """Cifras de lo que Spark ya dejó guardado en MongoDB (para el monitor de Kafka)."""
    ultimo = bd["alertas"].find_one(sort=[("lote", -1)], projection={"_id": 0, "lote": 1})
    return {
        "lecturas": bd["telemetria"].estimated_document_count(),
        "alertas": bd["alertas"].estimated_document_count(),
        "ultimo_lote": ultimo["lote"] if ultimo else 0,
    }


def estadisticas_topicos(consumidor, particiones, inicios):
    """Por tópico: cuántos mensajes almacena y el offset final de cada partición."""
    fines = consumidor.end_offsets(particiones)
    topicos = {}
    for tp in particiones:
        info = topicos.setdefault(tp.topic, {"particiones": {}, "total": 0})
        info["particiones"][str(tp.partition)] = fines[tp]
        info["total"] += fines[tp] - inicios[tp]
    return topicos


# --- Archivos del front-end (solo las carpetas necesarias, no el código Python) ---

@app.route("/")
def inicio():
    return send_from_directory(BASE_DIR, "index.html")


@app.route("/kafka")
def pagina_kafka():
    return send_from_directory(BASE_DIR, "kafka.html")


@app.route("/css/<path:archivo>")
def estilos(archivo):
    return send_from_directory(os.path.join(BASE_DIR, "css"), archivo)


@app.route("/js/<path:archivo>")
def scripts(archivo):
    return send_from_directory(os.path.join(BASE_DIR, "js"), archivo)


# --- API del mapa ---

@app.route("/api/estado")
def api_estado():
    """Foto instantánea (útil para depurar desde el navegador)."""
    return jsonify(leer_estado())


@app.route("/api/stream")
def api_stream():
    """Server-Sent Events: la conexión queda abierta y el servidor empuja datos cuando hay cambios."""
    def generador():
        ultimo = None
        while True:
            contenido = json.dumps(leer_estado(), default=str)
            if contenido != ultimo:
                yield f"data: {contenido}\n\n"
                ultimo = contenido
            time.sleep(INTERVALO_REVISION_SEG)

    return Response(
        stream_with_context(generador()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache"},
    )


# --- API del monitor de Kafka ---

@app.route("/api/kafka-stream")
def api_kafka_stream():
    """Lee los tópicos de Kafka como observador y empuja al navegador cada mensaje que pasa."""
    def generador():
        consumidor = None
        try:
            # Observador: asigna las particiones a mano, sin grupo y sin confirmar offsets,
            # así que NO interfiere con Spark.
            consumidor = KafkaConsumer(bootstrap_servers=KAFKA_SERVIDOR, enable_auto_commit=False)

            particiones = []
            for topico in TOPICOS_KAFKA:
                for numero in sorted(consumidor.partitions_for_topic(topico) or []):
                    particiones.append(TopicPartition(topico, numero))
            if not particiones:
                raise RuntimeError("No se encontraron los tópicos en Kafka (¿ya los creaste con kafka-topics.sh?)")

            consumidor.assign(particiones)
            consumidor.seek_to_end()                         # solo mensajes nuevos, desde ahora
            inicios = consumidor.beginning_offsets(particiones)

            while True:
                recibidos = consumidor.poll(timeout_ms=1000, max_records=300)

                mensajes = []
                for registros in recibidos.values():
                    for r in registros:
                        texto = r.value.decode("utf-8", errors="replace") if r.value else ""
                        try:
                            valor = json.loads(texto)
                        except ValueError:
                            valor = {"crudo": texto}
                        mensajes.append({
                            "topico": r.topic,
                            "particion": r.partition,
                            "offset": r.offset,
                            "clave": r.key.decode("utf-8", errors="replace") if r.key else None,
                            "valor": valor,
                            "marca_ms": r.timestamp,
                        })
                mensajes.sort(key=lambda m: (m["marca_ms"], m["offset"]))

                try:
                    destino = estado_destino()
                except Exception:
                    destino = None   # si MongoDB no responde, el monitor de Kafka sigue funcionando

                paquete = {
                    "mensajes": mensajes,
                    "topicos": estadisticas_topicos(consumidor, particiones, inicios),
                    "destino": destino,
                }
                yield f"data: {json.dumps(paquete, default=str)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': f'{type(e).__name__}: {e}'})}\n\n"
        finally:
            if consumidor is not None:
                consumidor.close()

    return Response(
        stream_with_context(generador()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache"},
    )


if __name__ == "__main__":
    print("[API] Mapa:  http://localhost:5000")
    print("[API] Kafka: http://localhost:5000/kafka")
    app.run(host="127.0.0.1", port=5000, debug=False, threaded=True)