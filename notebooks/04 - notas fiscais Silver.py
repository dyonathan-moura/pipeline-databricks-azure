# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.pandas as ps
from pyspark.sql.functions import to_date,col, date_format

# COMMAND ----------

from pyspark.sql import SparkSession

# Abrindo sessão Spark com Delta Lake configurado
spark = SparkSession.builder \
    .appName("ReadDelta") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Caminho para o arquivo Delta
delta_path = '/mnt/dados/bronze/planilha_controle_notas'

# Ler o arquivo Delta
df_delta = spark.read.format("delta").load(delta_path)

# Mostrar o DataFrame lido
df_delta.show(5)


# COMMAND ----------


