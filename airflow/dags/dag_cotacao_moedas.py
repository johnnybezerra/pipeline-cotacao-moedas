import os
import json
import requests
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
from minio import Minio
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator

# --- Configurações de Conexão dentro da Rede do Docker ---
MINIO_ENDPOINT   = os.getenv('MINIO_ENDPOINT', 'http://minio:9000').replace('http://', '').replace('https://', '')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio@1234!')
BUCKET           = 'moedas'

PG_CONFIG = {
    "dbname": os.getenv("DB_NAME", "moedas"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
    "host": os.getenv("DB_HOST", "postgres-data"),
    "port": os.getenv("DB_PORT", "5432")
}

URL_API = "https://economia.awesomeapi.com.br/last/USD-BRL,EUR-BRL,BTC-BRL"

def get_db_engine():
    return create_engine(
        f"postgresql://{PG_CONFIG['user']}:{PG_CONFIG['password']}@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['dbname']}"
    )

def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

# =============================================================
# TASKS DA DAG
# =============================================================

def pipeline_bronze():
    id_extracao = datetime.now().strftime('%Y%m%d%H%M%S')
    data_atualizacao = datetime.now()
    
    response = requests.get(URL_API, timeout=15)
    if response.status_code != 200:
        raise Exception(f"Erro na API: {response.status_code}")
        
    dados_brutos = response.json()
    
    # 1. Salva no Postgres (Staging Bronze)
    conn = psycopg2.connect(**PG_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.extracao_moedas_bronze (
            id_extracao VARCHAR(50), codigo_moeda VARCHAR(10), nome_moeda VARCHAR(100),
            valor_compra NUMERIC(15,4), valor_venda NUMERIC(15,4), alta NUMERIC(15,4),
            baixa NUMERIC(15,4), variacao NUMERIC(15,4), pct_mudanca NUMERIC(10,4),
            data_cotacao TIMESTAMP, data_atualizacao TIMESTAMP,
            PRIMARY KEY (id_extracao, codigo_moeda)
        );
    """)
    
    query_insert = """
        INSERT INTO public.extracao_moedas_bronze 
        (id_extracao, codigo_moeda, nome_moeda, valor_compra, valor_venda, alta, baixa, variacao, pct_mudanca, data_cotacao, data_atualizacao)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id_extracao, codigo_moeda) DO NOTHING;
    """
    for item in dados_brutos.values():
        cursor.execute(query_insert, (
            id_extracao, item.get('code'), item.get('name'), float(item.get('bid', 0)),
            float(item.get('ask', 0)), float(item.get('high', 0)), float(item.get('low', 0)),
            float(item.get('varBid', 0)), float(item.get('pctChange', 0)),
            item.get('create_date'), data_atualizacao
        ))
    conn.commit()
    cursor.close()
    conn.close()
    
    # 2. Salva JSON no MinIO
    client = get_minio_client()
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
        
    payload = {"id_extracao": id_extracao, "data_atualizacao": data_atualizacao.isoformat(), "dados_brutos": dados_brutos}
    conteudo = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    caminho = f"bronze/ano={data_atualizacao.year}/mes={data_atualizacao.month:02d}/dia={data_atualizacao.day:02d}/cotacao_{id_extracao}.json"
    client.put_object(BUCKET, caminho, BytesIO(conteudo), len(conteudo), content_type='application/json')


def pipeline_silver():
    engine = get_db_engine()
    df_bronze = pd.read_sql("SELECT * FROM public.extracao_moedas_bronze", engine)
    
    df_silver = df_bronze.drop_duplicates(subset=['id_extracao', 'codigo_moeda']).copy()
    df_silver['spread_venda_compra'] = df_silver['valor_venda'] - df_silver['valor_compra']
    df_silver['amplitude_diaria'] = df_silver['alta'] - df_silver['baixa']
    
    df_silver['data_atualizacao'] = pd.to_datetime(df_silver['data_atualizacao'])
    df_silver['ano'] = df_silver['data_atualizacao'].dt.year
    df_silver['mes'] = df_silver['data_atualizacao'].dt.month
    df_silver['dia'] = df_silver['data_atualizacao'].dt.day
    df_silver['hora'] = df_silver['data_atualizacao'].dt.hour
    
    # Esvazia a tabela Silver sem apagar a estrutura (preservando views associadas)
    with engine.begin() as conn:
        try:
            conn.execute(text("TRUNCATE TABLE public.extracao_moedas_silver CASCADE;"))
        except Exception:
            pass  # Ignora se a tabela ainda não existir na primeira execução
    
    df_silver.to_sql('extracao_moedas_silver', engine, schema='public', if_exists='append', index=False)


def pipeline_gold():
    engine = get_db_engine()
    df_silver = pd.read_sql("SELECT * FROM public.extracao_moedas_silver", engine)
    
    df_sorted = df_silver.sort_values(by=['codigo_moeda', 'data_atualizacao'])
    
    # 1. Tabela Diária
    df_diario = df_sorted.groupby(['codigo_moeda', 'nome_moeda', 'ano', 'mes', 'dia']).agg(
        qtd_coletas=('id_extracao', 'count'),
        preco_abertura=('valor_compra', 'first'),
        preco_fechamento=('valor_compra', 'last'),
        preco_medio=('valor_compra', 'mean'),
        preco_minimo=('valor_compra', 'min'),
        preco_maximo=('valor_compra', 'max'),
        spread_medio=('spread_venda_compra', 'mean'),
        ultima_atualizacao=('data_atualizacao', 'max')
    ).reset_index()
    
    # 2. KPI Atual
    df_kpi = df_sorted.groupby('codigo_moeda').last().reset_index()
    
    # 3. Elimina tabelas antigas com colunas incompatíveis e recria a estrutura
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS public.gold_cotacoes_diarias CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS public.gold_kpi_atual CASCADE;"))
    
    # 4. Salva a nova estrutura de colunas
    df_diario.to_sql('gold_cotacoes_diarias', engine, schema='public', if_exists='replace', index=False)
    df_kpi.to_sql('gold_kpi_atual', engine, schema='public', if_exists='replace', index=False)

# =============================================================
# DEFINIÇÃO DA DAG
# =============================================================

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'pipeline_cotacao_moedas_hoje',
    default_args=default_args,
    description='Pipeline Medallion (Bronze/Silver/Gold) Executado de Hora em Hora',
    schedule='0 * * * *',
    catchup=False,
    max_active_runs=1
) as dag:

    task_bronze = PythonOperator(
        task_id='camada_bronze',
        python_callable=pipeline_bronze
    )

    task_silver = PythonOperator(
        task_id='camada_silver',
        python_callable=pipeline_silver
    )

    task_gold = PythonOperator(
        task_id='camada_gold',
        python_callable=pipeline_gold
    )

    task_bronze >> task_silver >> task_gold