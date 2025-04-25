from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, explode, split, count
from pyspark.sql.window import Window

# 1. Crear sesión Spark
spark = SparkSession.builder.appName("movies").getOrCreate()

# 2. Leer los datos TSV
movies_df = spark.read.csv("movies.tsv", sep="\t", header=False, inferSchema=True).toDF("actor", "title", "year")
ratings_df = spark.read.csv("movie-ratings.tsv", sep="\t", header=False, inferSchema=True).toDF("rating", "title", "year")

# Convertir tipos
movies_df = movies_df.withColumn("year", col("year").cast("int"))
ratings_df = ratings_df.withColumn("year", col("year").cast("int"))

# 3. JOIN entre ambos
combined_df = movies_df.join(ratings_df, on=["title", "year"], how="inner")

# 4. Persistir el DataFrame
combined_df.persist()

# 5. Mostrar uso de memoria
print(f"\nNúmero de registros combinados: {combined_df.count()}")
combined_df.printSchema()

# 6. Película con mayor puntuación por año
window = Window.partitionBy("year").orderBy(col("rating").desc())
top_movies = combined_df.withColumn("rank", row_number().over(window)) \
                        .filter(col("rank") == 1) \
                        .select("year", "title", "rating")

print("\nPelículas con mayor puntuación por año:")
top_movies.show(truncate=False)

# 7. Lista de intérpretes de películas con mayor puntuación por año
interpretes_top = top_movies.join(movies_df, on=["title", "year"], how="left") \
                             .select("year", "title", "actor") \
                             .distinct()

print("\nIntérpretes de las películas mejor valoradas:")
interpretes_top.show(truncate=False)

# 8. Parejas de intérpretes que han trabajado juntos
# Agrupar actores por película
from pyspark.sql.functions import collect_set

pelicula_actores = movies_df.groupBy("title", "year").agg(collect_set("actor").alias("actores"))

# Crear combinaciones de parejas (cartesiano sobre actores de cada peli)
from itertools import combinations
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def generar_parejas(actores):
    return list(combinations(sorted(actores), 2)) if len(actores) >= 2 else []

parejas_udf = spark.udf.register("parejas_udf", generar_parejas, "array<struct<_1:string,_2:string>>")

parejas_df = pelicula_actores.withColumn("parejas", parejas_udf(col("actores"))) \
                             .select(explode("parejas").alias("pareja")) \
                             .select(col("pareja._1").alias("interprete1"), col("pareja._2").alias("interprete2")) \
                             .groupBy("interprete1", "interprete2") \
                             .agg(count("*").alias("cantidad")) \
                             .orderBy(col("cantidad").desc()) \
                             .limit(3)

print("\nTop 3 parejas de actores que más veces han trabajado juntos:")
parejas_df.show(truncate=False)
