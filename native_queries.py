from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Crear la sesión de Spark
spark = SparkSession.builder.appName("native_queries").getOrCreate()

# Leer los archivos con estructura conocida
movies_df = spark.read.csv("movies.tsv", sep="\t", header=False, inferSchema=True)\
    .toDF("actor", "title", "year")
ratings_df = spark.read.csv("movie-ratings.tsv", sep="\t", header=False, inferSchema=True)\
    .toDF("rating", "title", "year")

# Convertir año a int (algunas veces se lee como string)
ratings_df = ratings_df.withColumn("year", col("year").cast("int"))
movies_df = movies_df.withColumn("year", col("year").cast("int"))

# JOIN por título y año
df_joined = movies_df.join(ratings_df, on=["title", "year"], how="inner")

# Consulta 1: Película con mayor puntuación por año
window = Window.partitionBy("year").orderBy(col("rating").desc())
top_movies = df_joined.withColumn("rank", row_number().over(window)) \
                      .filter(col("rank") == 1) \
                      .select("year", "title", "rating")

print("Películas con mayor puntuación por año:")
top_movies.show(truncate=False)

# Consulta 2: Media de puntuación por actor
avg_rating_by_actor = df_joined.groupBy("actor").avg("rating") \
                               .withColumnRenamed("avg(rating)", "avg_rating")
print("Media de puntuación por actor:")
avg_rating_by_actor.show(truncate=False)

# Consulta 3: Películas con rating > 7
high_rated = df_joined.filter(col("rating") > 7.0) \
                      .select("title", "rating", "actor", "year")
print("Películas con rating > 7:")
high_rated.show(truncate=False)
