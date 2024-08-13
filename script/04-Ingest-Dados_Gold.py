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

from pyspark.sql import functions as F

def create_gold_layer(output_silver, output_gold):
    # Lê os dados da camada Silver
    df_silver = spark.read.format("delta").load(output_silver)
    
    # Cria uma visão agregada com a quantidade de cervejarias por tipo e localização
    df_gold = df_silver.groupBy("brewery_type", "country", "state", "city") \
        .agg(F.count("*").alias("brewery_count")) \
        .orderBy("country", "state", "city", "brewery_type")
    
    # Adiciona uma coluna com a data de atualização
    df_gold = df_gold.withColumn("updated_at", F.current_timestamp())
    
    # Salva os dados na camada Gold
    df_gold.write.mode("overwrite") \
        .format("delta") \
        .save(output_gold)
    
    print(f"Dados da camada Gold salvos com sucesso no caminho {output_gold}")

# Chamada da função
create_gold_layer(output_silver, output_gold)

# COMMAND ----------

df_gold = spark.read.format("delta").load(output_gold)