from pyspark.sql import SparkSession

# Iniciar sesión Spark
spark = SparkSession.builder.appName("table_data_frame").getOrCreate()

# Leer los archivos TSV como DataFrames
movies_df = spark.read.csv("movies.tsv", sep="\t", header=True, inferSchema=True)
ratings_df = spark.read.csv("movie-ratings.tsv", sep="\t", header=True, inferSchema=True)

# Mostrar una muestra de los datos
print("Películas:")
movies_df.show(5)
print("Ratings:")
ratings_df.show(5)

# Unirlos como ejemplo si comparten alguna columna (puede ser movieId)
# Supongamos que la clave común es 'movieId'
joined_df = movies_df.join(ratings_df, on="movieId", how="inner")

# Mostrar resultado final
joined_df.show()
