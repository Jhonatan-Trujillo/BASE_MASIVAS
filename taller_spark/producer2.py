import json
import time

from kafka import KafkaProducer

from vehiculos import generar_satelites

KAFKA_SERVIDOR = "localhost:9092"
TOPICO = "telemetria-satelites"
INTERVALO_SEG = 1


def main():
    flota = generar_satelites(2)

    try:
        productor = KafkaProducer(
            bootstrap_servers=KAFKA_SERVIDOR,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except Exception as e:
        raise SystemExit(f"[Productor 2] No se pudo conectar a Kafka en {KAFKA_SERVIDOR}: {e}\n"
                         "¿Está encendido el contenedor 'kafka'? Revisa con: docker ps")

    print(f"[Productor 2] Enviando telemetría de {len(flota)} satélites al tópico '{TOPICO}'")

    try:
        while True:
            for vehiculo in flota:
                vehiculo.actualizar()
                datos = vehiculo.a_dict()
                productor.send(TOPICO, key=vehiculo.id, value=datos)
                print(f"[Productor 2] {vehiculo.id} | alt {datos['altitud'] / 1000:.1f} km | "
                      f"vel {datos['velocidad']} km/h | bat {datos['bateria']}%")
            productor.flush()
            time.sleep(INTERVALO_SEG)
    except KeyboardInterrupt:
        print("\n[Productor 2] Detenido.")
    finally:
        productor.close()


if __name__ == "__main__":
    main()