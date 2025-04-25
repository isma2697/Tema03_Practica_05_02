from pyspark.sql import SparkSession

# Crear sesión de Spark
spark = SparkSession.builder.appName("sql_queries").getOrCreate()

# Leer los TSVs
movies_df = spark.read.csv("movies.tsv", sep="\t", header=False, inferSchema=True)\
    .toDF("actor", "title", "year")
ratings_df = spark.read.csv("movie-ratings.tsv", sep="\t", header=False, inferSchema=True)\
    .toDF("rating", "title", "year")

# Convertir tipos
movies_df = movies_df.withColumn("year", movies_df["year"].cast("int"))
ratings_df = ratings_df.withColumn("year", ratings_df["year"].cast("int"))

# Registrar vistas temporales
movies_df.createOrReplaceTempView("movies")
ratings_df.createOrReplaceTempView("ratings")

# Consulta 1: Película con mayor puntuación por año
query1 = spark.sql("""
    SELECT year, title, MAX(rating) as rating
    FROM ratings
    GROUP BY year, title
    ORDER BY year
""")
print("Película con mayor puntuación por año:")
query1.show(truncate=False)

# Consulta 2: Media de puntuación por actor
query2 = spark.sql("""
    SELECT m.actor, AVG(r.rating) as avg_rating
    FROM movies m
    JOIN ratings r ON m.title = r.title AND m.year = r.year
    GROUP BY m.actor
""")
print("Media de puntuación por actor:")
query2.show(truncate=False)

# Consulta 3: Películas con rating > 7
query3 = spark.sql("""
    SELECT r.title, r.rating, m.actor, r.year
    FROM ratings r
    JOIN movies m ON r.title = m.title AND r.year = m.year
    WHERE r.rating > 7.0
""")
print("Películas con rating > 7:")
query3.show(truncate=False)
