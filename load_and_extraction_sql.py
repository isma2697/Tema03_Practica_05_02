from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_timestamp, month, year
import matplotlib.pyplot as plt

# Crear sesión de Spark
spark = SparkSession.builder.appName("Load and Extraction SQL").getOrCreate()

# Configurar parser de fecha para versiones nuevas de Spark
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Leer CSV inferido
df = spark.read.option("header", True).option("inferSchema", True).csv("salesdata/salesdata/*.csv")

# Limpieza inicial
# Eliminar filas nulas
df = df.na.drop()

# Eliminar filas que tienen "Product" como valor de producto (cabecera repetida)
df = df.filter(df.Product != "Product")

# Crear columnas City y State
# Separar dirección: "136 Church St, New York City, NY 10001"
df = df.withColumn("City", split(col("Purchase Address"), ", ").getItem(1))
df = df.withColumn("State", split(split(col("Purchase Address"), ", ").getItem(2), " ").getItem(1))

# Transformar fecha en Timestamp
# El formato es "MM/dd/yy HH:mm"
df = df.withColumn("OrderTimestamp", to_timestamp(col("Order Date"), "MM/dd/yy HH:mm"))

# Crear columnas de Año y Mes
df = df.withColumn("Year", year(col("OrderTimestamp")))
df = df.withColumn("Month", month(col("OrderTimestamp")))

# Confirmar que haya datos
print(f"Total filas después de limpieza: {df.count()}")

# Guardar en Parquet particionado por Year y Month
df.write.mode("overwrite").partitionBy("Year", "Month").parquet("salesoutput")

print(" Datos guardados en formato Parquet particionado.")

# Leer solo los datos del año 2019
df_2019 = spark.read.parquet("salesoutput/Year=2019")

# Crear vista temporal para consultas SQL
df_2019.createOrReplaceTempView("sales")

# -------------------------------------------
# 1. Mes con mayor recaudacion
query1 = """
SELECT Month, SUM(`Quantity Ordered` * `Price Each`) AS TotalVentas
FROM sales
GROUP BY Month
ORDER BY TotalVentas DESC
"""

resultado1 = spark.sql(query1)
resultado1.show()

# Visualizar en un grafico
pdf1 = resultado1.toPandas()
plt.figure()
plt.bar(pdf1['Month'], pdf1['TotalVentas'])
plt.xlabel('Mes')
plt.ylabel('Ventas Totales')
plt.title('Ventas totales por mes')
plt.show()
# -------------------------------------------
# 2. Top 10 ciudades que más unidades han vendido
query2 = """
SELECT City, SUM(`Quantity Ordered`) as TotalUnidades
FROM sales
GROUP BY City
ORDER BY TotalUnidades DESC
LIMIT 10
"""
resultado2 = spark.sql(query2)
resultado2.show()

# Visualizar en un gráfico
pdf2 = resultado2.toPandas()
plt.figure(figsize=(10,6))
plt.barh(pdf2['City'], pdf2['TotalUnidades'])
plt.xlabel('Unidades Vendidas')
plt.ylabel('Ciudad')
plt.title('Top 10 Ciudades por Unidades Vendidas')
plt.gca().invert_yaxis()  # poner la ciudad con más ventas arriba
plt.grid(True)
plt.show()

# -------------------------------------------
# 3. Cantidad de pedidos por horas (sólo pedidos de más de un producto)
query3 = """
SELECT hour(OrderTimestamp) AS Hora, COUNT(*) AS NumPedidos
FROM sales
WHERE Product IS NOT NULL
GROUP BY Hora
ORDER BY Hora
"""
resultado3 = spark.sql(query3)
resultado3.show()

# Visualizar en un gráfico
pdf3 = resultado3.toPandas()
plt.figure(figsize=(10,6))
plt.plot(pdf3['Hora'], pdf3['NumPedidos'], marker='o')
plt.xlabel('Hora del Día')
plt.ylabel('Cantidad de Pedidos')
plt.title('Cantidad de Pedidos por Hora')
plt.grid(True)
plt.xticks(range(24))  # marcar todas las horas
plt.show()

# -------------------------------------------
# 4. Productos comprados juntos en pedidos de NY
query4 = """
SELECT `Order ID`, collect_set(Product) AS Productos
FROM sales
WHERE State = 'NY'
GROUP BY `Order ID`
HAVING size(Productos) > 1
"""
resultado4 = spark.sql(query4)
resultado4.show(truncate=False)

# -------------------------------------------
# Cerrar la sesión de Spark
spark.stop()
