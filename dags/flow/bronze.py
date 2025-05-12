from airflow.utils.task_group import TaskGroup
from scripts.bronze.download_files import DownloadFiles
from scripts.bronze.download_aux_files import DownloadAuxiliares
from scripts.bronze.get_years import GetYears
from scripts.bronze.updates import CheckUpdates
from tools.pipeline import Pipeline
from airflow.decorators import task
from airflow.operators.python import BranchPythonOperator
import logging

# Função para obter anos
@task
def obter_anos():
    """Obtém a lista de anos disponíveis na fonte de dados."""
    getter = GetYears()
    anos = getter.get_years(Pipeline.SOURCE_URL)
    logging.info(f"Ano(s) disponíveis encontrados: {anos}")
    return anos

# Funções de download
@task
def baixar_exportacao(ano: int):
    """Baixa os arquivos de exportação do ano informado."""
    downloader = DownloadFiles()
    downloader.download_exportacao(ano)
    logging.info(f"Exportação de {ano} baixada com sucesso.")

@task
def baixar_importacao(ano: int):
    """Baixa os arquivos de importação do ano informado."""
    downloader = DownloadFiles()
    downloader.download_importacao(ano)
    logging.info(f"Importação de {ano} baixada com sucesso.")

@task
def download_auxiliares():
    """Realiza download das tabelas auxiliares."""
    download = DownloadAuxiliares()
    download.baixar_arquivos()
    logging.info("Tabelas auxiliares baixadas com sucesso.")

# Task para verificar a necessidade de atualização
@task.branch
def verificar_atualizacao():
    """Verifica se há atualização disponível na fonte."""
    logging.info("Iniciando verificação de atualização...")
    checker = CheckUpdates()
    if checker.precisa_atualizar:
        logging.info("Atualização necessária. Prosseguindo com o pipeline.")
        return 'bronze.obter_anos'  # precisa do task_id COMPLETO da task a ser chamada!
    else:
        logging.info("Banco de dados já atualizado. Finalizando DAG.")
        return 'end'

def camada_bronze():
    with TaskGroup("bronze", tooltip="Grupo de Tarefas Bronze") as bronze_group:
        anos_task = obter_anos()
        exportacoes = baixar_exportacao.expand(ano=anos_task)
        importacoes = baixar_importacao.expand(ano=anos_task)
        download_aux_task = download_auxiliares()

        verificar_atualizacao() >> anos_task >> exportacoes >> importacoes >> download_aux_task

    return bronze_group




