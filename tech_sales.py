from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, split, to_timestamp, year, month

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("tech_sales") \
    .getOrCreate()

# Solución al problema de formato de fecha
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

#1. Lectura de los datos con inferencia de esquema
df_inferido = spark.read.option("header", True).option("inferSchema", True).csv("salesdata/salesdata/*.csv")
print("\Leyendo con esquema inferido")
df_inferido.printSchema()

#2. Lectura de los datos usando esquema manual
schema = StructType([
    StructField("Order ID", StringType(), True),
    StructField("Product", StringType(), True),
    StructField("Quantity Ordered", IntegerType(), True),
    StructField("Price Each", DoubleType(), True),
    StructField("Order Date", StringType(), True),
    StructField("Purchase Address", StringType(), True)
])

df = spark.read.option("header", True).schema(schema).csv("salesdata/salesdata/*.csv")

print("\Leyendo con esquema manual")
df.printSchema()

#3. Limpieza de datos: quitar espacios en nombres de columnas
for col_name in df.columns:
    df = df.withColumnRenamed(col_name, col_name.replace(" ", ""))

#4. Eliminar filas nulas
df = df.dropna()

#5. Eliminar filas que son cabeceras dentro de los datos
# (donde 'OrderID' == 'Order ID')
df = df.filter(df.OrderID != "Order ID")

#6. Extraer City y State desde PurchaseAddress
address_split = split(col("PurchaseAddress"), ", ")
df = df.withColumn("City", address_split.getItem(1))
df = df.withColumn("State", split(address_split.getItem(2), " ").getItem(0))

#7. Convertir OrderDate a Timestamp
df = df.withColumn("OrderTimestamp", to_timestamp("OrderDate", "MM/dd/yy HH:mm"))

#8. Crear columnas de Mes y Año
df = df.withColumn("Month", month("OrderTimestamp"))
df = df.withColumn("Year", year("OrderTimestamp"))

# Mostrar los primeros registros
df.show(5, truncate=False)

# Parar la sesión de Spark
spark.stop()
