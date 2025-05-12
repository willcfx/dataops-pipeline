import os
import json
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import logging
import psycopg2
from psycopg2.extras import execute_batch
from sqlalchemy import Integer, String, DateTime
from tools.pipeline import Pipeline
from tools.schema import SCHEMA


# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AuxLoader:
    def __init__(self, parquet_folder=Pipeline.STORAGE_AUX, json_path=Pipeline.JSON_PATH):
        self.parquet_folder = parquet_folder
        self.json_path = json_path

        # Mapeamento entre nome da tabela e nome da pasta
        self.table_to_folder = {
            'silver.d_blocos': 'BLOCOS',
            'silver.d_ncm': 'NCM',
            'silver.d_ncm_isic': 'NCM_ISIC',
            'silver.d_ncm_sh': 'NCM_SH',
            'silver.d_pais': 'PAIS',
            'silver.d_uf': 'UF',
            'silver.d_unidade': 'UNIDADE',
            'silver.d_urf': 'URF',
            'silver.d_via': 'VIA'
        }

        # Carrega a configuração do banco de dados
        self.db_config = self.load_db_config(json_path)

        # Chama o método para criar a engine do SQLAlchemy
        self.engine = self.create_sqlalchemy_engine(self.db_config)

        # Realiza a conexão para garantir que está tudo certo
        self.test_connection()

    def create_sqlalchemy_engine(self, db_config):
        """Cria uma engine do SQLAlchemy para conexão com o banco de dados."""
        try:
            # String de conexão com o banco de dados
            connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            logger.info(f"Connection: {connection_string}")
            engine = create_engine(connection_string)
            logger.info("Engine do SQLAlchemy criada com sucesso.")
            return engine
        except Exception as e:
            logger.error(f"Erro ao criar a engine do SQLAlchemy: {e}")
            raise

    def load_db_config(self, json_path):
        """Carrega as configurações do banco de dados de um arquivo JSON."""
        try:
            with open(json_path, 'r') as f:
                db_config = json.load(f)
            return db_config[0]
        except Exception as e:
            logger.error(f"Erro ao carregar configurações do banco de dados: {e}")
            raise

    def test_connection(self):
        """Testa a conexão com o banco de dados."""
        try:
            with self.engine.connect() as conn:
                logger.info("Conexão com o banco de dados bem-sucedida.")
        except Exception as e:
            logger.error(f"Erro ao conectar ao banco de dados: {e}")
            raise

    def read_parquet_files(self, folder_name):
        """Lê o arquivo .parquet da pasta correspondente à tabela e retorna um DataFrame."""
        file_path = os.path.join(self.parquet_folder, folder_name, f'{folder_name}.parquet')

        if not os.path.exists(file_path):
            raise Exception(f"Arquivo {file_path} não encontrado para a tabela {folder_name}.")

        logger.info(f"Lendo arquivo: {file_path}")
        df = pd.read_parquet(file_path)
        return df

    def transform_data(self, df, table_name):
        """Aplica transformações nos dados antes de carregar no banco."""
        # Converte os nomes das colunas para minúsculas
        df.columns = [col.lower() for col in df.columns]

        # Aplica o tipo de dado correto de acordo com o schema
        for column in df.columns:
            if column in SCHEMA[table_name]:
                expected_type = SCHEMA[table_name][column]
                # Converte o tipo de dado da coluna
                if expected_type == Integer:
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                elif expected_type == String:
                    df[column] = df[column].astype(str)
                elif expected_type == DateTime:
                    df[column] = pd.to_datetime(df[column], errors='coerce')

        # Adiciona a coluna de data de carga
        df['data_carga'] = datetime.now()

        # # Log para verificar o tipo de dados após a transformação
        # logger.info(f"Schema após transformação dos dados: {df.dtypes}")

        return df

    def load_to_postgres(self, df, table_name, replace_table=False):
        """Carrega o DataFrame no banco de dados Postgres, com a opção de substituir a tabela."""
        try:
            # Conectar ao banco usando psycopg2
            conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                dbname=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            cursor = conn.cursor()

            # Se o flag replace_table for True, limpa a tabela antes de inserir
            if replace_table:
                cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;")  # Limpa a tabela e reinicia os IDs
                conn.commit()
                logger.info(f"Tabela {table_name} limpa com sucesso.")

            # Converte as colunas de tipo Timestamp para string no formato desejado
            for col in df.select_dtypes(include=['datetime64']).columns:
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M')

            # Prepara a consulta de inserção com placeholders para cada coluna
            columns = df.columns.tolist()
            placeholders = ', '.join(['%s'] * len(columns))
            insert_query = f"""
                INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})
                """

            # Converte os dados para uma lista de tuplas
            data = [tuple(row) for row in df.values.tolist()]

            # Debug logs (mantenha para ajudar na depuração)
            logger.info(f"Consulta SQL: {insert_query}")
            logger.info(f"Schema dos dados: {df.dtypes}")
            logger.info(f"Dados a serem inseridos (primeiros 5): {data[:1]}")

            # Realiza a inserção em lotes
            execute_batch(cursor, insert_query, data, page_size=1000)
            conn.commit()

            logger.info(f"Dados carregados com sucesso na tabela {table_name}")
        except Exception as e:
            logger.error(f"Erro ao carregar os dados na tabela {table_name}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def process_table(self, table_name, replace_table=False):
        """Processa uma tabela específica, realizando leitura, transformação e carga."""
        # Verifica se existe um mapeamento para a tabela
        if table_name not in self.table_to_folder:
            raise ValueError(f"Não há mapeamento definido para a tabela {table_name}.")

        # Pasta onde os arquivos .parquet estão localizados
        folder_name = self.table_to_folder[table_name]

        # Ler os dados, transformar e carregar no banco
        try:
            df = self.read_parquet_files(folder_name)
            df_transformed = self.transform_data(df, table_name)
            self.load_to_postgres(df_transformed, table_name, replace_table)
        except Exception as e:
            logger.error(f"Erro ao processar a tabela {table_name}: {e}")
            raise

    def run(self, replace_table=False):
        """Executa o pipeline completo para todas as tabelas mapeadas."""
        for table_name in self.table_to_folder:
            logger.info(f"Iniciando o processamento para a tabela {table_name}...")
            self.process_table(table_name, replace_table)
