### Dados na Cloud Azure ###

Este projeto tem como objetivo simular uma pipeline de dados completa, seguindo boas práticas de Engenharia de Dados, desde a ingestão até a persistência em Cloud e Banco de Dados.

A solução foi construída em Python, com foco em qualidade de dados, estrutura profissional e preparação para ambientes produtivos.

## 🎯 Objetivo do Projeto ##

Simular um cenário real de engenharia de dados onde:

📂 Arquivos locais são ingeridos

🔄 Dados são transformados em DataFrames

✅ Qualidade e schema são validados com Pandera

☁️ Dados são enviados para a Azure Blob Storage em formato Parquet

🗄️ Dados são persistidos em um Banco de Dados relacional via SQLAlchemy

## 🧱 Arquitetura da Pipeline ##

Fluxo lógico do processamento:

Arquivos Locais
      ↓
Pandas DataFrames
      ↓
Validação com Pandera
      ↓
Parquet (Azure Blob Storage)
      ↓
Insert no Banco de Dados


## 🛠️ Tecnologias Utilizadas ##

Python 3.10+

Pandas – Manipulação de dados

Pandera – Validação de schema e qualidade de dados

SQLAlchemy – ORM e inserts controlados no banco

Azure Blob Storage – Armazenamento em nuvem

Parquet – Formato otimizado para dados analíticos

dotenv – Gerenciamento de variáveis de ambiente

## ▶ Como Executar o Projeto: ##

1️⃣ Instalar as dependências
'pip install -r requirements.txt'

2️⃣ Configurar variáveis de ambiente

Crie e preencha o arquivo .env_dev com as credenciais necessárias, como:

- Azure Blob Storage

- Banco de Dados

- Variáveis de ambiente do projeto

3️⃣ Executar a pipeline
'python main.py'

**⏱️ Tempo de Execução**

- ⌛ Primeira carga (full load): aproximadamente 15 minutos

_O tempo é influenciado principalmente pela volumetria dos dados e pela escrita inicial no banco_

## ✅ Boas Práticas Aplicadas ##

- Validação de dados antes da persistência

- Separação clara de responsabilidades

- Uso de ORM para controle de tipos e defaults

- Logs estruturados para observabilidade

- Arquitetura preparada para evolução (incremental, Airflow, dbt, etc.)

## 🔮 Próximos Passos (Evolução do Projeto) ##

- Implementar carga incremental

- Orquestração com Airflow

- Camadas Bronze / Silver / Gold

- Testes automatizados de dados com dbt

- Monitoramento e alertas

**👨‍💻 Autor: Nicolas César Nagel | Engenheiro de Dados / Analista de Dados**

_Projeto desenvolvido com foco em aprendizado prático, engenharia de dados moderna e padrões de mercado, utilizando a stack Azure._