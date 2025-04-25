from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, min as min_

# Crear sesión de Spark
spark = SparkSession.builder.appName("sales").getOrCreate()

# Leer archivo CSV con nulos
df = spark.read.option("header", True).option("inferSchema", True).csv("VentasNulos.csv")

# 1. Eliminar filas con al menos 4 nulos
df = df.withColumn("null_count", sum(col(c).isNull().cast("int") for c in df.columns))
df_cleaned = df.filter(col("null_count") < 4).drop("null_count")

# 2. Calcular media de ventas (sin nulos) y mínimo de euros
media_ventas = df_cleaned.select(avg("Ventas")).first()[0]
media_ventas_redondeado = int(round(media_ventas)) if media_ventas is not None else 0

min_euros = df_cleaned.select(min_("Euros")).first()[0]
min_euros = min_euros if min_euros is not None else 0

# 3. Sustituir valores nulos
df_final = df_cleaned \
    .withColumn("Nombre", when(col("Nombre").isNull(), "Empleado").otherwise(col("Nombre"))) \
    .withColumn("Ventas", when(col("Ventas").isNull(), media_ventas_redondeado).otherwise(col("Ventas"))) \
    .withColumn("Euros", when(col("Euros").isNull(), min_euros).otherwise(col("Euros"))) \
    .withColumn("Ciudad", when(col("Ciudad").isNull(), "C.V.").otherwise(col("Ciudad"))) \
    .withColumn("Identificador", when(col("Identificador").isNull(), "XYZ").otherwise(col("Identificador")))

# Mostrar resultado
df_final.show(truncate=False)
