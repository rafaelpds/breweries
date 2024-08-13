# Breweries Case

## 1 - Arquitetura Proposta para o Case Breweries

1. Azure Data Factory (ADF) para orquestração
2. Azure Databricks para processamento de dados
3. Azure Data Lake Storage Gen2 (ADLS Gen2) para armazenamento
4. Azure Key Vault para gerenciamento seguro de segredos
5. Azure Monitor para monitoramento e alertas

## 2- Configurações do Ambiente

### 1. Criação da Subscription
- Nome da subscription: "Breweries-Case"

### 2. Integração de Recursos Azure

#### 2.1 Azure Databricks
- Resource Group: "breweries-prd"
- Workspace: "adb-breweries-prd"

#### 2.2 Azure Data Lake Storage Gen2
- Nome da Storage Account: "adlsbreweriesprd"
- Habilitar: "Enable hierarchical namespace"

#### 2.3 Azure Data Factory (ADF)

#### 2.4 Integrações
- ADF + ADLS Gen2: Criar Linked Service no ADF usando Gen2
- ADF + Databricks: 
  - Criar Linked Service na opção Compute do ADF
  - Conceder permissão para a conta de serviço do ADF acessar o Databricks

#### 2.5 Azure Key Vault
- Configurar scope no Databricks para armazenar chaves
- URL: `https://<databricks-instance>#secrets/createScope`

### 3. Montagem do Data Lake
1. Criar App Registration
2. Criar segredo
3. Conceder permissão IAM de "Storage Blob Data Contributor"
4. Ajustar código para montagem

### 4. Configurações Adicionais
- Configurar Resource Groups conforme necessário
- Ajustar permissões e acessos entre serviços

## Monitoramento e Alertas

Para a parte de monitoramento utilizei o próprio monitor da Azure que é integrado com o Azure Data Factory e também pode comunicar a equipe de monitoramento quando ocorrer alguma falha, abaixo segue como fazer as configurações: 

### Azure Monitor
1. No portal Azure, acesse "Monitor"
2. Crie um novo dashboard
3. Adicione gráficos e métricas relevantes, eu escolhi a métrica: "Failed Pipeline Runs", por exemplo.

### Configuração de Alertas
1. No próprio dashboard das métricas no Azure Monitor, tem a opção "New Alert Rule" é possível configurar uma alerta para as métricas criadas e adicionar a forma de comunicação, eu escolhi a comunicação via e-mail.


### 3 - Execução

1. Após configurar todo o ambiente, no Azure Data Factory selecione os notebooks databricks, faça as configurações requisitadas, como: Linked Service, Path do Notebook e coloque cada um dos notebooks criados na ordem de execução, no próprio pipeline é possível configurar a opção de "retry" para que seja realizada uma nova tentativa de conexão, caso por exemplo perca a conexão com a API.


![Texto Alternativo](https://github.com/rafaelpds/breweries/blob/main/imagem/pipeline.png)

Por fim, com a prpoposta será possível criar um lakehouse seguindo a arquitetura medalion.

<a href="https://www.databricks.com/glossary/medallion-architecture">
<img src="https://raw.githubusercontent.com/Databricks-BR/open_tax/main/images/medalhao.png" width="800px"></a>


