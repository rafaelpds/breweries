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

from pyspark.sql.functions import col, current_timestamp

def ingest_bronze_and_save(input_landing, output_bronze):
    # Lê o arquivo
    df = spark.read.json(input_landing)
    
    # Transforma
    df = df.withColumn('bronze_ingestion_timestamp', current_timestamp())
    
    # Salva
    df.write.mode('overwrite') \
        .option('overwriteSchema','True') \
        .format('delta') \
        .save(output_bronze)

    print(f"Dados salvos com sucesso no caminho {output_bronze}")

# Chamada da função
ingest_bronze_and_save(input_landing, output_bronze)

# COMMAND ----------

df = spark.read.format('delta').load(output_bronze)
display(df.limit(5))

# COMMAND ----------

df.count()