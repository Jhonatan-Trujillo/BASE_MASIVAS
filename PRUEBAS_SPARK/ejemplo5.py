"""Genera ~100.000 líneas de texto para el lab"""
import random

palabras = ("spark hadoop datos nube python java scala "
            "cluster memoria disco batch streaming "
            "sql dataframe rdd modelo grafo kafka "
            "analitica etl lote real tiempo delta").split()

random.seed(42)
with open("cuento.txt", "w", encoding="utf-8") as f:
    for i in range(100_000):
        linea = " ".join(random.choices(palabras, k=30))
        f.write(linea + "\n")

print("cuento.txt listo ✔  (≈100.000 líneas)")