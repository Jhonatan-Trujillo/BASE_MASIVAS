import json
import time

from kafka import KafkaProducer

from vehiculos import generar_drones

KAFKA_SERVIDOR = "localhost:9092"
TOPICO = "telemetria-drones"
INTERVALO_SEG = 1


def main():
    flota = generar_drones(4)

    try:
        productor = KafkaProducer(
            bootstrap_servers=KAFKA_SERVIDOR,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except Exception as e:
        raise SystemExit(f"[Productor 1] No se pudo conectar a Kafka en {KAFKA_SERVIDOR}: {e}\n"
                         "¿Está encendido el contenedor 'kafka'? Revisa con: docker ps")

    print(f"[Productor 1] Enviando telemetría de {len(flota)} drones al tópico '{TOPICO}'")

    try:
        while True:
            for vehiculo in flota:
                vehiculo.actualizar()
                datos = vehiculo.a_dict()
                # La clave (id del vehículo) hace que sus mensajes siempre vayan a la misma partición
                productor.send(TOPICO, key=vehiculo.id, value=datos)
                print(f"[Productor 1] {vehiculo.id} | alt {datos['altitud']} m | "
                      f"vel {datos['velocidad']} km/h | bat {datos['bateria']}%")
            productor.flush()
            time.sleep(INTERVALO_SEG)
    except KeyboardInterrupt:
        print("\n[Productor 1] Detenido.")
    finally:
        productor.close()


if __name__ == "__main__":
    main()