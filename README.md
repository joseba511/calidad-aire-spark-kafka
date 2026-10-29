# Calidad del aire con Apache Spark y Kafka

Proyecto de la Tarea 3 del curso Big Data (UNAD). Analiza datos abiertos de calidad del aire de datos.gov.co, primero en batch con PySpark y después en tiempo real con Kafka y Spark Streaming.

**Autor:** José Edivier Beltrán Andrade

## Datos

Archivo CSV de datos.gov.co:
https://www.datos.gov.co/api/v3/views/dgnf-6h7v/query.csv

Tiene 43 filas y 23 columnas con mediciones de PM10 de 5 estaciones, del 2 al 29 de septiembre de 2026. La columna `cpm_g_m3` es la concentración de partículas.

## Archivos

| Archivo | Para qué sirve |
|---|---|
| `batch_calidad_aire.py` | Análisis en batch: carga, limpia, calcula promedios por estación y por mes, y guarda 3 gráficas en la carpeta `graficas` |
| `kafka_producer.py` | Lee los datos reales y los manda al topic `calidad_aire`, una fila por segundo |
| `spark_streaming_consumer.py` | Lee el topic con Spark Streaming, muestra todas las columnas y calcula cada minuto el promedio, el máximo y las veces que se pasa el límite |

## Requisitos

Máquina virtual Ubuntu con Spark 3.5.8, Python 3.10, Java 11 y Kafka 3.9.2 (Scala 2.12).

```
pip install kafka-python matplotlib pandas
```

## Cómo ejecutarlo

**1. Análisis en batch**

```
wget -O calidad_aire.csv "https://www.datos.gov.co/api/v3/views/dgnf-6h7v/query.csv"
spark-submit batch_calidad_aire.py
```

**2. Tiempo real**

Encender ZooKeeper y Kafka (desde la carpeta de Kafka):

```
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
bin/kafka-server-start.sh -daemon config/server.properties
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic calidad_aire
```

En una terminal, el productor:

```
python3 kafka_producer.py
```

En otra terminal, el consumidor:

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 spark_streaming_consumer.py
```

Para parar cualquiera de los dos, Ctrl+C.

## Resultados del batch

- Promedio más alto: Costa Verde, con 44,80 µg/m³. El más bajo: Pescaito, con 33,38.
- Ninguna de las 43 mediciones supera el límite diario de 75 µg/m³. La más alta fue 56,42 en Costa Verde el 20 de septiembre de 2026.
