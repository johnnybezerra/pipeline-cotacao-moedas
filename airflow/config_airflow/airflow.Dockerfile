FROM apache/airflow:2.8.1-python3.11

USER airflow

# Instala os pacotes exatos ignorando o requirements.txt para evitar conflitos
RUN pip install --no-cache-dir minio pandas requests psycopg2-binary pyarrow \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.11.txt"