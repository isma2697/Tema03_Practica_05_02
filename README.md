# Práctica 05.02 – Apache Spark

Este repositorio contiene la solución a la práctica 05.02 del Tema 03, centrada en el uso de **Apache Spark** para el procesamiento de datos. Se han utilizado tanto **Spark SQL** como el lenguaje nativo de **PySpark**, junto con funciones definidas por el usuario (UDF) y visualización de estadísticas con **Pandas**.

---

## 📂 Estructura del proyecto

```
.
├── table_data_frame.py              # Creación de DataFrames con tablas anteriores
├── native_queries.py                # Traducción de consultas SQL al lenguaje nativo de PySpark
├── sql_queries.py                   # Consultas utilizando Spark SQL
├── names.py                         # Transformaciones sobre datos de nombres desde JSON
├── sales.py                         # Limpieza de datos nulos y transformaciones sobre VentasNulos.csv
├── user_defined_function.py         # Uso de UDF en PySpark y Spark SQL
├── pandas.py                        # Visualización de estadísticas con Pandas
├── movies.py                        # Análisis y joins sobre datasets de películas
├── tech_sales.py                    # Lectura y limpieza de datos de ventas desde salesdata.zip
├── load_and_extraction_sql.py      # Almacenamiento en formato Parquet y consultas SQL
├── comandos.txt                     # Comandos para ejecutar todos los scripts
├── nombres.json                     # Archivo JSON de entrada para names.py
├── VentasNulos.csv                  # Dataset de ventas con nulos
├── movies.tsv                       # Dataset de películas
├── movie-ratings.tsv                # Dataset de puntuaciones de películas
├── salesdata.zip                    # Datos de ventas de 2019
```

---

## ✅ Objetivos de la práctica

- Crear y manipular DataFrames con Apache Spark.
- Realizar consultas usando lenguaje nativo de PySpark.
- Ejecutar consultas con Spark SQL.
- Usar funciones definidas por el usuario (UDF).
- Limpiar y transformar datos.
- Unir y analizar datasets.
- Almacenar datos en formato Parquet.
- Visualizar resultados con Pandas.

---

## 📅 Entrega

El proyecto debe ser comprimido en un archivo `.zip` con el siguiente nombre:

```
IAB_BIU_Tema_03_Practica_05_02_Nombre1_Apellidos1[_Nombre2_Apellidos2]
```

Incluyendo:
- Todos los scripts mencionados.
- Archivo `comandos.txt` con instrucciones de ejecución.
- Ficheros de entrada necesarios.

---

## 🔗 Enlace de referencia

[Material de apoyo de Spark](https://aitor-medrano.github.io/iabd/spark/spark.html)


