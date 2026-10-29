import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ARCHIVO = "calidad_aire.csv"
os.makedirs("graficas", exist_ok=True)

spark = SparkSession.builder.appName("CalidadAireBatch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", "4")

# 1. Cargar los datos
df = spark.read.csv(ARCHIVO, header=True, inferSchema=True)
print("Columnas originales:", df.columns)

# Nos quedamos solo con las columnas que sirven para el analisis
df = df.select("estacion", "parametro", "fecha", "ta_c", "pa", "cpm_g_m3") \
       .withColumnRenamed("cpm_g_m3", "concentracion")
print("Filas cargadas:", df.count())
df.printSchema()
df.show(5, truncate=False)

# 2. Limpieza
print("Valores nulos por columna:")
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

df = df.dropDuplicates()
df = df.dropna(subset=["estacion", "parametro", "fecha", "concentracion"])
df = df.withColumn("estacion", F.upper(F.trim(F.col("estacion"))))
df = df.withColumn("parametro", F.upper(F.trim(F.col("parametro"))))
df = df.withColumn("fecha", F.to_timestamp("fecha"))
df = df.filter(F.col("concentracion") >= 0)

# 3. Transformacion: columnas nuevas
df = df.withColumn("anio", F.year("fecha")).withColumn("mes", F.month("fecha"))
# Limites diarios de la norma colombiana (Res. 2254 de 2017), en ug/m3. Verificar antes de entregar.
df = df.withColumn(
    "limite",
    F.when(F.col("parametro") == "PM10", 75.0).when(F.col("parametro") == "PM2.5", 37.0)
)
df = df.withColumn("excede", F.when(F.col("concentracion") > F.col("limite"), 1).otherwise(0))
df.cache()
print("Filas despues de limpiar:", df.count())

# 4. Analisis exploratorio (EDA)
print("Resumen estadistico de la concentracion:")
df.select("concentracion").describe().show()

print("Promedio, maximo y numero de mediciones por estacion y parametro:")
por_estacion = df.groupBy("estacion", "parametro").agg(
    F.count("*").alias("mediciones"),
    F.round(F.avg("concentracion"), 2).alias("promedio"),
    F.round(F.max("concentracion"), 2).alias("maximo"),
    F.sum("excede").alias("veces_que_excede")
).orderBy(F.desc("promedio"))
por_estacion.show(truncate=False)

print("Promedio por mes:")
por_mes = df.groupBy("anio", "mes").agg(
    F.count("*").alias("mediciones"),
    F.round(F.avg("concentracion"), 2).alias("promedio")
).orderBy("anio", "mes")
por_mes.show(50)

print("Dias con valores mas altos:")
df.select("estacion", "parametro", "fecha", "concentracion") \
  .orderBy(F.desc("concentracion")).show(10, truncate=False)

# 5. Graficas (los resultados ya vienen agregados, por eso son pocos datos)
pdf_est = df.groupBy("estacion").agg(F.avg("concentracion").alias("promedio")) \
            .orderBy("promedio").toPandas()
plt.figure(figsize=(8, 4.5))
plt.barh(pdf_est["estacion"], pdf_est["promedio"], color="#1f6fb2")
plt.xlabel("Concentracion promedio (ug/m3)")
plt.title("Promedio de material particulado por estacion")
plt.tight_layout()
plt.savefig("graficas/promedio_por_estacion.png", dpi=150)
plt.close()

pdf_mes = por_mes.toPandas()
pdf_mes["periodo"] = pdf_mes["anio"].astype(str) + "-" + pdf_mes["mes"].astype(str).str.zfill(2)
plt.figure(figsize=(8, 4.5))
plt.plot(pdf_mes["periodo"], pdf_mes["promedio"], marker="o", color="#f0a02b")
plt.xticks(rotation=45)
plt.ylabel("Concentracion promedio (ug/m3)")
plt.title("Promedio mensual de material particulado")
plt.tight_layout()
plt.savefig("graficas/promedio_por_mes.png", dpi=150)
plt.close()

pdf_exc = df.groupBy("estacion").agg(F.sum("excede").alias("excedencias")) \
            .orderBy("excedencias").toPandas()
plt.figure(figsize=(8, 4.5))
plt.barh(pdf_exc["estacion"], pdf_exc["excedencias"], color="#c0392b")
plt.xlabel("Numero de mediciones sobre el limite")
plt.title("Mediciones que superan el limite diario")
plt.tight_layout()
plt.savefig("graficas/excedencias_por_estacion.png", dpi=150)
plt.close()

print("Listo. Las graficas quedaron en la carpeta 'graficas'.")
spark.stop()
