from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("CalidadAireStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", "4")

# Todas las columnas del archivo de calidad del aire (llegan como texto y luego se convierten)
columnas = ["estacion", "parametro", "muestra_no", "filtro_no", "fecha", "pri", "prf", "pravg",
            "pa", "ta_c", "ta_k", "po_pa", "qr_m3_min", "porcdif", "li_horometro_min",
            "lf_horometro_min", "t_min", "qstd_m3_min", "vstd_m3", "wi_gr", "wf_gr", "wn_gr",
            "cpm_g_m3", "timestamp"]

schema = StructType([StructField(c, StringType()) for c in columnas])

# Leer del topic de Kafka
crudo = (spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "calidad_aire")
         .option("startingOffsets", "latest")
         .load())

# Parsear el JSON: cada dato queda en su propia columna
datos = (crudo
         .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
         .select("data.*"))

# Convertir cada columna a su tipo
for c in ["muestra_no", "filtro_no"]:
    datos = datos.withColumn(c, F.col(c).cast("long"))
numericas = [c for c in columnas
             if c not in ["estacion", "parametro", "muestra_no", "filtro_no", "fecha", "timestamp"]]
for c in numericas:
    datos = datos.withColumn(c, F.col(c).cast("double"))
datos = datos.withColumn("fecha", F.to_timestamp("fecha"))
datos = datos.withColumn("momento", F.to_timestamp("timestamp"))

# 1) Tabla con todos los datos en columnas, tal como van llegando
tabla = datos.select(columnas)
query_tabla = (tabla.writeStream
               .outputMode("append")
               .format("console")
               .option("truncate", "false")
               .option("numRows", "20")
               .queryName("datos_completos")
               .trigger(processingTime="10 seconds")
               .start())

# 2) Estadisticas por ventana de 1 minuto, estacion y parametro
# Limites diarios (Res. 2254 de 2017): PM10 = 75 y PM2.5 = 37 ug/m3
con_exceso = datos.withColumn(
    "excede",
    F.when(((F.col("parametro") == "PM10") & (F.col("cpm_g_m3") > 75)) |
           ((F.col("parametro") == "PM2.5") & (F.col("cpm_g_m3") > 37)), 1).otherwise(0)
)
stats = (con_exceso
         .groupBy(F.window("momento", "1 minute"), "estacion", "parametro")
         .agg(F.count("*").alias("lecturas"),
              F.round(F.avg("cpm_g_m3"), 2).alias("promedio"),
              F.round(F.max("cpm_g_m3"), 2).alias("maximo"),
              F.sum("excede").alias("excedencias")))

query_stats = (stats.writeStream
               .outputMode("update")
               .format("console")
               .option("truncate", "false")
               .queryName("estadisticas_por_ventana")
               .trigger(processingTime="10 seconds")
               .start())

spark.streams.awaitAnyTermination()
