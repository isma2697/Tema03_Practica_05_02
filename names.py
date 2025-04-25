from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, monotonically_increasing_id

# Crear sesión de Spark
spark = SparkSession.builder.appName("names").getOrCreate()

# Leer el fichero nombres.json
df = spark.read.json("nombres.json")

# 1. Columna FaltanJubilacion (67 - Edad)
df = df.withColumn("FaltanJubilacion", 67 - col("Edad"))

# 2. Columna Apellidos con valor 'XYZ'
df = df.withColumn("Apellidos", lit("XYZ"))

# 3. Eliminar columnas Mayor30 y Apellidos
df = df.drop("Mayor30").drop("Apellidos")

# 4. Columna AnyoNac (2025 - Edad)
df = df.withColumn("AnyoNac", 2025 - col("Edad"))

# 5. Añadir campo Id incremental y reordenar columnas
df = df.withColumn("Id", monotonically_increasing_id())
df = df.select("Id", "Nombre", "Edad", "AnyoNac", "FaltanJubilacion", "Ciudad")

# Mostrar resultado
df.show(truncate=False)
