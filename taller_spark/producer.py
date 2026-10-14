import socket
import json
import time
import random
import math
from datetime import datetime, timezone

HOST = "localhost"
PUERTO = 9999

# Zona operativa visible en el mapa
LAT_MIN, LAT_MAX = 4.40, 4.46
LON_MIN, LON_MAX = -75.26, -75.20


class VehiculoAereo:
    """Clase base para cualquier vehículo que reporta telemetría ADS-B."""

    def __init__(self, id_vehiculo, tipo, lat, lon, altitud, velocidad, rumbo, bateria):
        self.id = id_vehiculo
        self.tipo = tipo                # dron o satelite
        self.lat = lat
        self.lon = lon
        self.altitud = altitud          # metros
        self.velocidad = velocidad      # km/h
        self.rumbo = rumbo              # grados 0-359
        self.bateria = bateria          # porcentaje 0-100

    def actualizar(self):
        """Simula el movimiento (dead reckoning simple) y el consumo de batería."""
        # Desplazamiento aproximado según velocidad y rumbo
        distancia = self.velocidad * (1 / 3600)  # km recorridos en 1 segundo
        rad = math.radians(self.rumbo)
        self.lat += (distancia / 111.32) * math.cos(rad)
        self.lon += (distancia / 111.32) * math.sin(rad)

        if self.tipo == "dron":
            if self.lat < LAT_MIN or self.lat > LAT_MAX:
                self.lat = max(LAT_MIN, min(LAT_MAX, self.lat))  # lo regresa dentro del límite
                self.rumbo = (180 - self.rumbo) % 360             # rebota (invierte componente lat)
            if self.lon < LON_MIN or self.lon > LON_MAX:
                self.lon = max(LON_MIN, min(LON_MAX, self.lon))
                self.rumbo = (-self.rumbo) % 360                  # rebota (invierte componente lon)

        # Altitud: si está por debajo del mínimo operativo, el autopiloto corrige y asciende;
        # si no, fluctúa libremente (ruido de sensor / turbulencia leve)
        if self.tipo == "dron" and self.altitud < 90:
            self.altitud += random.uniform(0.5, 3)
        else:
            self.altitud += random.uniform(-2, 2)

        self.rumbo = (self.rumbo + random.uniform(-3, 3)) % 360
        self.velocidad = max(0, self.velocidad + random.uniform(-1, 1))

        if self.tipo == "satelite":
            self.bateria = max(0, self.bateria - random.uniform(0.005, 0.02))
        else:
            self.bateria = max(0, self.bateria - random.uniform(0.05, 0.3))

    def a_json(self):
        return json.dumps({
            "id": self.id,
            "tipo": self.tipo,
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "altitud": round(self.altitud, 2),
            "velocidad": round(self.velocidad, 2),
            "rumbo": round(self.rumbo, 2),
            "bateria": round(self.bateria, 2),
            "timestamp": datetime.now(timezone.utc).isoformat()
        })


class Dron(VehiculoAereo):
    def __init__(self, id_vehiculo, lat, lon):
        super().__init__(
            id_vehiculo, "dron",
            lat, lon,
            altitud=random.uniform(80, 150),
            velocidad=random.uniform(20, 60),
            rumbo=random.uniform(0, 360),
            bateria=100.0
        )


class Satelite(VehiculoAereo):
    def __init__(self, id_vehiculo, lat, lon):
        super().__init__(
            id_vehiculo, "satelite",
            lat, lon,
            altitud=random.uniform(400000, 800000),  # órbita baja (LEO) en metros
            velocidad=random.uniform(27000, 28000),   # km/h típico LEO
            rumbo=random.uniform(0, 360),
            bateria=random.uniform(60, 100)
        )


def generar_flota(n_drones=4, n_satelites=2):
    flota = []
    for i in range(n_drones):
        flota.append(Dron(f"DRON-{i+1:02d}", lat=4.43 + random.uniform(-0.02, 0.02),
                           lon=-75.23 + random.uniform(-0.02, 0.02)))
    for i in range(n_satelites):
        flota.append(Satelite(f"SAT-{i+1:02d}", lat=4.43 + random.uniform(-0.02, 0.02),
                               lon=-75.23 + random.uniform(-0.02, 0.02)))
    return flota


def main():
    flota = generar_flota()

    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind((HOST, PUERTO))
    servidor.listen(1)
    print(f"[Productor] Esperando conexión de Spark en {HOST}:{PUERTO}...")

    conexion, direccion = servidor.accept()
    print(f"[Productor] Spark conectado desde {direccion}")

    try:
        while True:
            for vehiculo in flota:
                vehiculo.actualizar()
                linea = vehiculo.a_json() + "\n"
                conexion.sendall(linea.encode("utf-8"))
                print(f"[Productor] Enviado -> {linea.strip()}")
            time.sleep(1)
    except (BrokenPipeError, ConnectionResetError):
        print("[Productor] Spark cerró la conexión.")
    finally:
        conexion.close()
        servidor.close()


if __name__ == "__main__":
    main()