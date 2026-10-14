import json
import math
import random
from datetime import datetime, timezone

# Zona operativa visible en el mapa (debe coincidir con LAT_MIN/MAX, LON_MIN/MAX de script.js)
LAT_MIN, LAT_MAX = 4.40, 4.46
LON_MIN, LON_MAX = -75.26, -75.20


class VehiculoAereo:
    """Clase base para cualquier vehículo que reporta telemetría ADS-B."""

    def __init__(self, id_vehiculo, tipo, lat, lon, altitud, velocidad, rumbo, bateria):
        self.id = id_vehiculo
        self.tipo = tipo                # "dron" o "satelite"
        self.lat = lat
        self.lon = lon
        self.altitud = altitud          # metros
        self.velocidad = velocidad      # km/h
        self.rumbo = rumbo              # grados 0-359
        self.bateria = bateria          # porcentaje 0-100

    def actualizar(self):
        """Simula el movimiento (dead reckoning simple) y el consumo de batería."""
        distancia = self.velocidad * (1 / 3600)  # km recorridos en 1 segundo
        rad = math.radians(self.rumbo)
        self.lat += (distancia / 111.32) * math.cos(rad)
        self.lon += (distancia / 111.32) * math.sin(rad)

        # Solo los drones están confinados a la cobertura de las estaciones (rebotan en el borde)
        if self.tipo == "dron":
            if self.lat < LAT_MIN or self.lat > LAT_MAX:
                self.lat = max(LAT_MIN, min(LAT_MAX, self.lat))
                self.rumbo = (180 - self.rumbo) % 360
            if self.lon < LON_MIN or self.lon > LON_MAX:
                self.lon = max(LON_MIN, min(LON_MAX, self.lon))
                self.rumbo = (-self.rumbo) % 360

        # Altitud: si el dron está bajo el mínimo operativo, el autopiloto corrige y asciende
        if self.tipo == "dron" and self.altitud < 90:
            self.altitud += random.uniform(0.5, 3)
        else:
            self.altitud += random.uniform(-2, 2)

        self.rumbo = (self.rumbo + random.uniform(-3, 3)) % 360
        self.velocidad = max(0, self.velocidad + random.uniform(-1, 1))

        # Los satélites tienen paneles solares -> se descargan mucho más lento
        if self.tipo == "satelite":
            self.bateria = max(0, self.bateria - random.uniform(0.005, 0.02))
        else:
            self.bateria = max(0, self.bateria - random.uniform(0.05, 0.3))

    def a_dict(self):
        return {
            "id": self.id,
            "tipo": self.tipo,
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "altitud": round(self.altitud, 2),
            "velocidad": round(self.velocidad, 2),
            "rumbo": round(self.rumbo, 2),
            "bateria": round(self.bateria, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def a_json(self):
        return json.dumps(self.a_dict())


class Dron(VehiculoAereo):
    def __init__(self, id_vehiculo, lat, lon):
        super().__init__(
            id_vehiculo, "dron",
            lat, lon,
            altitud=random.uniform(80, 150),
            velocidad=random.uniform(20, 60),
            rumbo=random.uniform(0, 360),
            bateria=100.0,
        )


class Satelite(VehiculoAereo):
    def __init__(self, id_vehiculo, lat, lon):
        super().__init__(
            id_vehiculo, "satelite",
            lat, lon,
            altitud=random.uniform(400000, 800000),  # órbita baja (LEO) en metros
            velocidad=random.uniform(27000, 28000),   # km/h típico LEO
            rumbo=random.uniform(0, 360),
            bateria=random.uniform(60, 100),
        )


def generar_drones(cantidad=4):
    return [
        Dron(f"DRON-{i + 1:02d}",
             lat=4.43 + random.uniform(-0.02, 0.02),
             lon=-75.23 + random.uniform(-0.02, 0.02))
        for i in range(cantidad)
    ]


def generar_satelites(cantidad=2):
    return [
        Satelite(f"SAT-{i + 1:02d}",
                 lat=4.43 + random.uniform(-0.02, 0.02),
                 lon=-75.23 + random.uniform(-0.02, 0.02))
        for i in range(cantidad)
    ]