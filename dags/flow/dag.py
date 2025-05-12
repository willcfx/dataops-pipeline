from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from flow.bronze import camada_bronze
from flow.silver import camada_silver
from flow.gold import camada_gold

from scripts.bronze.start import DatabaseInitializer

import logging

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

def inicializar_banco():
    """Inicializa o banco de dados usando o script SQL fornecido."""
    db_init = DatabaseInitializer()
    sql_file = '/opt/airflow/dags/tools/table_scripts/table_script.sql'
    db_init.initialize_tables(sql_file)
    db_init.close()
    logging.info("Banco inicializado com sucesso.")

# DAG principal
with DAG(
    "DAG_COMEX_ESTADOS",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description="Pipeline para atualização dos dados de Comércio Exterior por Estado",
) as dag:

    end = EmptyOperator(task_id='end')

    # Task para inicializar o banco
    inicializar_banco_task = PythonOperator(
        task_id='inicializar_banco',
        python_callable=inicializar_banco
    )

    bronze = camada_bronze()
    silver = camada_silver()
    gold = camada_gold()

    # Definir o fluxo geral da DAG
    inicializar_banco_task >> bronze >> silver >> gold >> end
