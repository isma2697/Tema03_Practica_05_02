from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

# 1. Crear sesión Spark
spark = SparkSession.builder.appName("pandas").getOrCreate()

# 2. Leer y limpiar datos (relleno básico para evitar errores)
df = spark.read.option("header", True).option("inferSchema", True).csv("VentasNulos.csv")
df = df.fillna({
    "Nombre": "Empleado",
    "Ciudad": "C.V.",
    "Identificador": "XYZ",
    "Ventas": 0,
    "Euros": 0
})

# 3. Convertir a Pandas
pandas_df = df.toPandas()

# 4. Mostrar estadísticas
print("\n--- Estadísticas de Ventas ---")
print(pandas_df["Ventas"].describe())

print("\n--- Estadísticas de Euros ---")
print(pandas_df["Euros"].describe())

# 5. (Opcional) Gráfico: histograma de ventas
pandas_df["Ventas"].hist(bins=8)
plt.title("Distribución de Ventas")
plt.xlabel("Unidades vendidas")
plt.ylabel("Frecuencia")
plt.grid(True)
plt.tight_layout()
plt.savefig("ventas_histograma.png")  # Guarda el gráfico como imagen
plt.show()
