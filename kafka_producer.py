import csv
import io
import json
import time
import urllib.request
from datetime import datetime
from kafka import KafkaProducer

URL = "https://www.datos.gov.co/api/v3/views/dgnf-6h7v/query.csv"
ARCHIVO_LOCAL = "calidad_aire.csv"
TOPIC = "calidad_aire"


def leer_datos():
    # Primero intenta bajar los datos del link; si no puede, usa el archivo que ya descargamos
    try:
        pedido = urllib.request.Request(URL, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(pedido, timeout=60) as r:
            texto = r.read().decode("utf-8")
        print("Datos descargados desde el link")
    except Exception as e:
        print("No se pudo usar el link (" + str(e) + "), se usa el archivo local")
        with open(ARCHIVO_LOCAL, encoding="utf-8") as f:
            texto = f.read()
    return list(csv.DictReader(io.StringIO(texto)))


def a_numero(valor):
    try:
        return float(valor)
    except (TypeError, ValueError):
        return None


def armar_mensaje(fila):
    # Mandamos todas las columnas del archivo (menos las internas que empiezan con ":")
    mensaje = {k: v for k, v in fila.items() if k and not k.startswith(":")}
    if a_numero(mensaje.get("cpm_g_m3")) is None:
        return None
    mensaje["estacion"] = (mensaje.get("estacion") or "").strip().upper()
    mensaje["parametro"] = (mensaje.get("parametro") or "").strip().upper()
    # la hora de envio es la actual, para que Spark pueda armar las ventanas de 1 minuto
    mensaje["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return mensaje


filas = leer_datos()
print("Filas leidas:", len(filas))

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

print("Enviando datos reales al topic", TOPIC, "(Ctrl+C para parar)")
try:
    vuelta = 1
    while True:
        for fila in filas:
            mensaje = armar_mensaje(fila)
            if mensaje is None:
                continue
            producer.send(TOPIC, value=mensaje)
            print("Enviado:", mensaje)
            time.sleep(1)
        print("Se terminaron los datos, empezando la vuelta", vuelta + 1)
        vuelta += 1
except KeyboardInterrupt:
    producer.flush()
    producer.close()
    print("Productor detenido")
