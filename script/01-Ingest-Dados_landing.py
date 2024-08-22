# Databricks notebook source
# MAGIC %md
# MAGIC ### Version Code Control
# MAGIC
# MAGIC | versão | data | autor | e-mail | alterações |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1.0 | 12-AGO-2024 | Rafael Santos | rafaelpdsantos@gmail.com | Primeira versão  |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Descrição
# MAGIC
# MAGIC | projeto | aplicação | módulo | tabela | objetivo |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | Acadêmico | DB Open Breweries | ETL Landing | Diversas JSON | Ingestão dos dados de cervejarias em JSON |

# COMMAND ----------

# MAGIC %md
# MAGIC <a href="https://www.databricks.com/glossary/medallion-architecture">
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/open_tax/main/images/medalhao.png" width="800px"></a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Coleta dos Dados

# COMMAND ----------

# DBTITLE 1,Ingesting Data from Brewery API
# Importamos requests para fazer chamadas HTTP à API
import requests
# Importamos json para manipular dados no formato JSON
import json

def coleta_cervejarias(directory_path):
    all_breweries = []
    page = 1
    
    while True:
        url = f"https://api.openbrewerydb.org/v1/breweries?page={page}&per_page=200"
        response = requests.get(url)
        
        if response.status_code != 200:
            print(f"Erro na requisição: {response.status_code}")
            break
        
        breweries = response.json()

        if not breweries:
            break
        
        all_breweries.extend(breweries)
        page += 1
    
    file_path = f'{directory_path}/breweries.json'
    
    breweries_json = json.dumps(all_breweries)
    
    # Usamos dbutils.fs.put para salvar o arquivo JSON no Databricks File System
    # Isso permite salvar os dados coletados de forma persistente
    dbutils.fs.put(file_path, breweries_json, overwrite=True)
    
    print(f"Dados gravados com sucesso em: {file_path}")

directory_path = '/mnt/adlsadventureworkprd/lading-zone/brewery/breweries'

coleta_cervejarias(directory_path)

# COMMAND ----------

df = spark.read.json('/mnt/adlsadventureworkprd/lading-zone/brewery/breweries/breweries.json')

display(df.limit(5))

# COMMAND ----------

df.count()
