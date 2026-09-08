# Pipeline de Cotação de Moedas (Data Engineering)

Este projeto consiste em um pipeline de dados automatizado para extração, processamento e visualização das cotações diárias de moedas em tempo real. A arquitetura segue o modelo de camadas (Bronze, Silver e Gold) em um Data Lake/Warehouse containerizado.

---

## Tecnologias Utilizadas

* **Orquestração:** Apache Airflow
* **Data Lake (Storage):** MinIO (S3 Compatible)
* **Data Warehouse / Banco Relacional:** PostgreSQL
* **Data Visualization / BI:** Metabase
* **Containerização:** Docker & Docker Compose
* **Linguagem:** Python / Jupyter Notebooks

---

## Arquitetura do Pipeline

1. **Camada Bronze (Ingestão):** Extração dos dados da API de cotação de moedas e salvamento dos arquivos brutos no MinIO.
2. **Camada Silver (Tratamento):** Limpeza, estruturação, conversão de tipos de dados e salvamento no formato adequado.
3. **Camada Gold (Modelagem):** Agregação dos dados e persistência no banco PostgreSQL para consumo do BI.
4. **Visualização:** Dashboards criados no Metabase conectados ao PostgreSQL.

---

## Como Executar o Projeto

### Pré-requisitos
* [Docker Desktop](https://www.docker.com/) instalado e rodando.
* [Git](https://git-scm.com/) instalado.

### Passo a Passo

1. **Clonar o repositório:**
   ```bash
   git clone [https://github.com/johnnybezerra/pipeline-cotacao-moedas.git](https://github.com/johnnybezerra/pipeline-cotacao-moedas.git)
   cd pipeline-cotacao-moedas


```text
cotacao/
├── airflow/                          # Configurações e Dockerfile do Airflow
│   └── dags/                         # DAGs de orquestração (ex: dag_cotacao_moedas.py)
├── 01_extracao_bronze_cotacao.ipynb  # Notebook de exploração / ingestão (Bronze)
├── 02_pipeline_silver_cotacao.ipynb  # Notebook de transformação (Silver)
├── 03_consumer_gold_cotacao.ipynb    # Notebook de carga/modelagem (Gold)
├── docker-compose.yml                # Orquestração dos containers Docker
├── env.example                       # Exemplo de variáveis de ambiente (sem senhas)
├── requirements.txt                  # Dependências Python do projeto
└── README.md                         # Documentação do projeto
