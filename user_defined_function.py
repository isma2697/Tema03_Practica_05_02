from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Crear Spark session
spark = SparkSession.builder.appName("user_defined_function").getOrCreate()

# Leer archivo limpio
df = spark.read.option("header", True).option("inferSchema", True).csv("VentasNulos.csv")

# Limpiar nulos básicos para este ejemplo (mínimo viable)
df = df.fillna({
    "Nombre": "Empleado",
    "Ciudad": "C.V.",
    "Identificador": "XYZ",
    "Ventas": 0,
    "Euros": 0
})

# -------------------------
# 1. Crear UDF en PySpark
# -------------------------

def clasificar_ventas(v):
    if v is None:
        return "Desconocido"
    elif v < 4:
        return "Bajo"
    elif v < 8:
        return "Medio"
    else:
        return "Alto"

clasificacion_udf = udf(clasificar_ventas, StringType())

# Aplicar UDF en una nueva columna
df_udf = df.withColumn("NivelVentas", clasificacion_udf(df["Ventas"]))
df_udf.show(truncate=False)

# -------------------------------------
# 2. Registrar UDF para usar en SQL
# -------------------------------------

spark.udf.register("clasificar_ventas", clasificar_ventas, StringType())
df_udf.createOrReplaceTempView("ventas")

df_sql = spark.sql("""
    SELECT Nombre, Ventas, clasificar_ventas(Ventas) AS NivelVentas_SQL
    FROM ventas
""")

df_sql.show(truncate=False)
