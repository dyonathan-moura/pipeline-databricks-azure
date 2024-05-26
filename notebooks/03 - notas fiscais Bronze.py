# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.pandas as ps
from pyspark.sql.functions import to_date,col, date_format

# COMMAND ----------

# Caminho para ler o arquivo
path = '/mnt/dados/inbound/planilha_controle_notas'
# Abrindo sessão Spark
spark = SparkSession.builder\
    .appName("ReadCsv")\
    .getOrCreate()

# Ler um arqivo com PySpark 
df = spark.read\
    .option('header','true')\
    .option('inferSchema','true')\
    .csv(path)

# Conversão de Spark DataFrame para pandas-on-Spark DataFrame
df_pandas = df.to_pandas_on_spark()

# COMMAND ----------

df_pandas.info()

# COMMAND ----------

# Renomear colunas
df = df.withColumnRenamed('Nota Fiscal', 'nota_fiscal')\
    .withColumnRenamed('Valor', 'valor')\
    .withColumnRenamed('Ordem de Compra', 'ordem_compra')\
    .withColumnRenamed('Descrição Fornecedor', 'descricao_fornecedor')\
    .withColumnRenamed('Código Fornecedor', 'codigo_fornecedor')\
    .withColumnRenamed('Filial', 'filial')\
    .withColumnRenamed('Data de Pagamento', 'data_pagamento')\
    .withColumnRenamed('Forma de Pagamento', 'forma_pagamento')\
    .withColumnRenamed('Existe Rateio', 'existe_rateio')\
    .withColumnRenamed('Anexo Boleto', 'anexo_boleto')\
    .withColumnRenamed('Patrimônio', 'patrimonio')

# Converter "Data de Pagamento" para tipo Data
df = df.withColumn('data_pagamento', to_date(col('data_pagamento'), 'yyyy-MM-dd'))
# df = df.withColumn('data_pagamento', date_format(col('data_pagamento'), 'dd-MM-yyyy'))


# COMMAND ----------

df.select('data_pagamento').show(5)

# COMMAND ----------

df.printSchema()


# COMMAND ----------

# Salvar o Dataframe no formato Delta
df.write.format('delta').mode('overwrite').save('/mnt/dados/bronze/planilha_controle_notas')
