from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType

# 1. Crear sesión de Spark para Batch
spark = SparkSession.builder \
    .appName("Sensores_Batch_Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Cargar el dataset histórico directamente desde el archivo local CSV
file_path = "sensores_historicos.csv"
print("\n=== 1. CARGA DEL HISTÓRICO DE SENSORES (LOCAL) ===")
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(file_path)
)

print("Cantidad inicial de registros:", df.count())
df.printSchema()

# 3. Limpieza de datos
print("\n=== 2. LIMPIEZA DE DATOS ===")
df = df.dropDuplicates()

df = df.withColumn("sensor_id", F.col("sensor_id").cast(IntegerType())) \
       .withColumn("temperature", F.col("temperature").cast(DoubleType())) \
       .withColumn("humidity", F.col("humidity").cast(DoubleType()))

df = df.filter(
    F.col("temperature").isNotNull() & F.col("humidity").isNotNull()
)

print("Registros después de la limpieza:", df.count())

# 4. Análisis Exploratorio de Datos
print("\n=== 3. ESTADÍSTICAS GENERALES ===")
df.select("temperature", "humidity").describe().show()

print("\n=== 4. PROMEDIO DE TEMPERATURA Y HUMEDAD POR SENSOR ===")
df.groupBy("sensor_id") \
  .agg(
      F.round(F.avg("temperature"), 2).alias("temp_promedio"),
      F.round(F.avg("humidity"), 2).alias("humedad_promedio"),
      F.count("*").alias("total_lecturas")
  ) \
  .orderBy("sensor_id") \
  .show(truncate=False)

spark.stop()
print("\nProcesamiento batch finalizado con éxito.")
