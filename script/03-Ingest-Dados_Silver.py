# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ### Version Code Control
# MAGIC
# MAGIC | versão | data | autor | e-mail | alterações |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1.0 | 12-AGO-2024 | Rafael Santos | rafaelpdsantos@gmail.com | Primeira versão  |

# COMMAND ----------

# MAGIC %run "./00-Config"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, trim

df_bronze = spark.read.format('delta').load(output_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verificando se existe algum registro com espaço em branco

# COMMAND ----------

from pyspark.sql.functions import trim
display(df_bronze.filter(col('country') !=trim(col('country'))))

# COMMAND ----------

display(df_bronze.filter(col('state') !=trim(col('state'))))

# COMMAND ----------

#Após identificar que existem registros com espaço em branco apliquei o trim em todo o DF
df_trim = df_bronze.select([trim(col(i)).alias(i) for i in df_bronze.columns])

display(df_trim.limit(50))

# COMMAND ----------

display(df_trim.filter(col('city') !=trim(col('city'))))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### Verificando se há valores duplicados

# COMMAND ----------

df_trim.groupBy('name', 'postal_code', 'phone', 'city', 'state').count().filter(col('count') > 1).show()

# COMMAND ----------

from pyspark.sql import DataFrame

# Definir todos os filtros como DataFrames separados
df1 = df_trim.filter(
    (col('name') == 'Wooden Robot') & 
    (col('postal_code') == '28203-4263') & 
    (col('phone') == '7723594993') & 
    (col('city') == 'Charlotte') & 
    (col('state') == 'North Carolina')
)

df2 = df_trim.filter(
    (col('name') == 'Philipsburg Brewing Co') & 
    (col('postal_code') == '59858') & 
    (col('phone') == '4068592739') & 
    (col('city') == 'Philipsburg') & 
    (col('state') == 'Montana')
)

df3 = df_trim.filter(
    (col('name') == 'One Well Brewing') & 
    (col('postal_code') == '49001-5264') & 
    (col('phone') == '2694599240') & 
    (col('city') == 'Kalamazoo') & 
    (col('state') == 'Michigan')
)

df4 = df_trim.filter(
    (col('name') == 'Ballast Point Brewing Co') & 
    (col('postal_code') == '92121-2405') & 
    (col('phone') == '8586952739') & 
    (col('city') == 'San Diego') & 
    (col('state') == 'California')
)

# Unir todos os DataFrames em um único DataFrame
df_result = df1.union(df2).union(df3).union(df4)

# Exibir o resultado
display(df_result)

# COMMAND ----------

#O critério para remover foram as linhas que tinham mais informações nulas
df_tratado = df_trim.filter((col('id') != 'c346bd50-0c32-4700-816e-b8aa8d111630') &
               (col('id') != '8d111536-e72d-4f2e-86b6-f3c1585e21a4') &
               (col('id') != '59764d11-cd2b-4d85-8e4c-a7d64cdc5754'))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def save_silver_partitioned_by_location(output_silver):

    # Salva os dados na camada Silver, particionando pela localização (estado ou cidade)
    df_tratado.write.mode('overwrite') \
        .option('overwriteSchema','True') \
        .partitionBy('country', 'state', 'city') \
        .format('delta') \
        .save(output_silver)

    print(f"Dados salvos com sucesso no caminho {output_silver}, particionados por estado e cidade.")

# Chamada da função
save_silver_partitioned_by_location(output_silver)
