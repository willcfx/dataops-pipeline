from airflow.utils.task_group import TaskGroup
from scripts.gold.fato_comex_estados import GoldComercioExteriorLoader
from airflow.decorators import task
import logging

@task
def fato_comex_estados():
    """Cria a tabela unificada fato_comex_estados na camada Gold."""
    loader = GoldComercioExteriorLoader()
    loader.run()
    logging.info("Tabela fato_comex_estados carregada com sucesso na camada Gold.")

def camada_gold():
    with TaskGroup("gold", tooltip="Grupo de Tarefas Gold") as gold_group:
        fato = fato_comex_estados()
        fato
    return gold_group
