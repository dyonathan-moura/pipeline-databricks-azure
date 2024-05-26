# Databricks notebook source
import numpy as np
import pandas as pd
from faker import Faker
from pyspark.sql import SparkSession

# COMMAND ----------

# Configurar o Faker e a Sessão Spark
faker = Faker()
spark = SparkSession.builder.appName("Salvar DataFrame").getOrCreate()

size = 10000  # Tamanho do DataFrame

# Gerar dados
notas_fiscais = np.random.permutation(np.arange(1, 20001))[:size]
oc = np.random.permutation(np.arange(1, 20001))[:size]
valores = np.random.uniform(low=89.90, high=99999.90, size=size).round(2)
descricao_fornecedores = [faker.company() for _ in range(size)]
cod_fornecedores = np.random.choice(range(1, 10000), size=size, replace=True)
filiais = np.random.randint(low=1, high=3000, size=size)
datas_pagamento = [faker.date_between(start_date='-2y', end_date='today') for _ in range(size)]
formas_pagamento = [faker.random.choice(["Boleto", "Cartão de Crédito", "Débito Automático", "Pix", "Cheque", "Dinheiro"]) for _ in range(size)]
contexto = ["SIM", "NÃO"]
rateio = [faker.random.choice(contexto) for _ in range(size)]
anexo_boleto = [faker.random.choice(contexto) for _ in range(size)]
patrimonio = [faker.random.choice(contexto) for _ in range(size)]

# Compilar todos os dados em um DataFrame pandas
df = pd.DataFrame({
    'Nota Fiscal': notas_fiscais,
    'Valor': valores,
    'Ordem de Compra': oc,
    'Descrição Fornecedor': descricao_fornecedores,
    'Código Fornecedor': cod_fornecedores,
    'Filial': filiais,
    'Data de Pagamento': datas_pagamento,
    'Forma de Pagamento': formas_pagamento,
    'Existe Rateio': rateio,
    'Anexo Boleto': anexo_boleto,
    'Patrimônio': patrimonio
})

# Converter para DataFrame Spark
sdf = spark.createDataFrame(df)

# Salvar o DataFrame do Spark
(sdf.write
   .format("csv")
   .option("header", "true")
   .option("compression", "gzip")
   .mode("overwrite")
   .save("/mnt/dados/inbound/planilha_controle_notas"))


# COMMAND ----------

dbutils.fs.ls('/mnt/dados/inbound/')
