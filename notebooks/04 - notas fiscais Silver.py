# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.pandas as ps
from pyspark.sql.functions import to_date,col, date_format,monotonically_increasing_id

# COMMAND ----------

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

# COMMAND ----------

delta_path = '/mnt/dados/silver/planilha_controle_notas/'
# Criar a tabela Fornecedores
fornecedores = df_delta.select('codigo_fornecedor','descricao_fornecedor').distinct()

# Extrair dados únicos de formas de pagamento
formas_pagamentos = df_delta.select('forma_pagamento').distinct()
formas_pagamentos = formas_pagamentos.withColumn('codigo_forma_pagamento', monotonically_increasing_id() + 1)

# Extrair dados únicos Filiais
filiais  = df_delta.select('filial','CNPJ').distinct()

# Criar a tabela Ordens de Compras
ordens_compras = df_delta.select('filial','ordem_compra').distinct()
ordens_compras = ordens_compras.withColumn('id_ordem', monotonically_increasing_id() )# Adicionar a coluna id_ordem com valores únicos


# Salvar Dataframe 
fornecedores.write.format("delta").mode("overwrite").save(delta_path +'fornecedor_delta') # Salvar datafreme fornecedores 
formas_pagamentos.write.format("delta").mode("overwrite").save(delta_path+'formas_pagamentos_delta') # Salvar Dataframe formas_pagamento 
ordens_compras.write.format('delta')\
    .option('mergeSchema','true')\
    .mode('overwrite')\
    .save(delta_path+'ordens_compras_delta') # Salvar Dataframe Ordens de compras

filiais.write.format('delta').mode('overwrite').save(delta_path+'filiais_delta') # Salvar Dataframe formas_pagamento 




# COMMAND ----------

# Substituir valores de forma_pagamento pelo código
notas_fiscais = df_delta.join(formas_pagamentos, on='forma_pagamento', how='left')

# Substituir valores de fornecedor pelo código
notas_fiscais = notas_fiscais.join(fornecedores, on='codigo_fornecedor', how='left')

# Substituir valores de filial pelo código
notas_fiscais = notas_fiscais.join(filiais, on='filial', how='left')

# Substituir valores de ordens de compra pelo id_ordem
notas_fiscais = notas_fiscais.join(ordens_compras, on='ordem_compra',how='left')

# Selecionar as colunas desejadas e renomear
notas_fiscais = notas_fiscais.select(
    col("nota_fiscal"),
    col("valor"),
    col("id_ordem"),
    col("codigo_fornecedor"),
    col("codigo_forma_pagamento"),
    col("data_pagamento"),
    col("existe_rateio"),
    col("anexo_boleto"),
    col("patrimonio")
)

# Dropar colunas desnecessárias
notas_fiscais = notas_fiscais.drop("forma_pagamento", "descricao_fornecedor", "CNPJ")




# COMMAND ----------

notas_fiscais.show(10,truncate=True)
