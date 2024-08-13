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
    # Inicializamos uma lista vazia para armazenar todas as cervejarias
    # Isso nos permite acumular dados de múltiplas páginas da API
    all_breweries = []
    # Começamos com a página 1 da API
    # Isso é necessário para a paginação dos resultados
    page = 1
    
    # Usamos um loop infinito para continuar buscando páginas até não haver mais dados
    while True:
        # Construímos a URL da API com o número da página e quantidade de resultados por página
        # Isso permite que busquemos os dados de forma paginada
        url = f"https://api.openbrewerydb.org/v1/breweries?page={page}&per_page=200"
        # Fazemos uma requisição GET para a URL da API
        # Isso nos retorna os dados das cervejarias para a página atual
        response = requests.get(url)
        
        # Verificamos se a requisição foi bem-sucedida (código 200)
        # Isso nos ajuda a identificar e tratar erros na chamada da API
        if response.status_code != 200:
            print(f"Erro na requisição: {response.status_code}")
            break
        
        # Convertemos a resposta JSON da API em um objeto Python
        # Isso nos permite trabalhar com os dados como uma lista de dicionários
        breweries = response.json()
        # Se não há mais cervejarias, saímos do loop
        # Isso indica que chegamos ao fim dos dados disponíveis
        if not breweries:
            break
        
        # Adicionamos as cervejarias desta página à nossa lista principal
        # Isso nos permite acumular todos os dados em uma única lista
        all_breweries.extend(breweries)
        
        # Incrementamos o número da página para a próxima iteração
        # Isso nos permite buscar a próxima página de resultados no próximo ciclo
        page += 1
    
    # Definimos o caminho completo onde o arquivo será salvo
    # Isso combina o diretório base com o nome do arquivo
    file_path = f'{directory_path}/breweries.json'
    
    # Convertemos toda a lista de cervejarias para uma string JSON
    # Isso prepara os dados para serem salvos em um arquivo
    breweries_json = json.dumps(all_breweries)
    
    # Usamos dbutils.fs.put para salvar o arquivo JSON no Databricks File System
    # Isso permite salvar os dados coletados de forma persistente
    dbutils.fs.put(file_path, breweries_json, overwrite=True)
    
    # Imprimimos uma mensagem confirmando onde os dados foram salvos
    # Isso fornece feedback ao usuário sobre o sucesso da operação
    print(f"Dados gravados com sucesso em: {file_path}")

# Exemplo de uso da função:
# Definimos o diretório onde o arquivo será salvo
# Isso especifica o local no DBFS onde queremos armazenar nossos dados
directory_path = '/mnt/adlsadventureworkprd/lading-zone/brewery/breweries'
# Chamamos a função para coletar e salvar os dados
# Isso inicia todo o processo de coleta e salvamento dos dados
coleta_cervejarias(directory_path)

# COMMAND ----------

# DBTITLE 1,Validação resultado da execução
#Verificando o resultado da execução
df = spark.read.json('/mnt/adlsadventureworkprd/lading-zone/brewery/breweries/breweries.json')

display(df.limit(5))

# COMMAND ----------

# DBTITLE 1,Validação resultado da execução
df.count()