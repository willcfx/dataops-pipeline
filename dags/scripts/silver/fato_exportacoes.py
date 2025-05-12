import os
import json
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import logging
import psycopg2
from psycopg2.extras import execute_batch
from tools.pipeline import Pipeline
from tools.schema import SCHEMA  # Garantir que o SCHEMA esteja disponível
from sqlalchemy.types import Integer, String, DateTime  # Tipos de dados para transformação

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExportacoesLoader:

    def __init__(self, parquet_folder=Pipeline.STORAGE_EXP, table_name=Pipeline.TABLE_F_EXPORTACOES_ESTADOS, json_path=Pipeline.JSON_PATH):
        self.parquet_folder = parquet_folder
        self.table_name = table_name
        self.json_path = json_path

        # Carrega a configuração do banco de dados
        self.db_config = self.load_db_config(json_path)

        # Chama o método para criar a engine do SQLAlchemy
        self.engine = self.create_sqlalchemy_engine(self.db_config)

        # Realiza a conexão para garantir que está tudo certo
        self.test_connection()

    def load_db_config(self, json_path):
        """Carrega as configurações do banco de dados de um arquivo JSON."""
        try:
            with open(json_path, 'r') as f:
                db_config = json.load(f)
            return db_config[0]
        except Exception as e:
            logger.error(f"Erro ao carregar configurações do banco de dados: {e}")
            raise

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

    def test_connection(self):
        """Testa a conexão com o banco de dados."""
        try:
            with self.engine.connect() as conn:
                logger.info("Conexão com o banco de dados bem-sucedida.")
        except Exception as e:
            logger.error(f"Erro ao conectar ao banco de dados: {e}")
            raise

    def read_parquet_files(self):
        """Lê todos os arquivos .parquet da pasta e subpastas, e concatena em um DataFrame único."""
        all_files = []
        for root, dirs, files in os.walk(self.parquet_folder):
            for file in files:
                if file.endswith('.parquet'):
                    all_files.append(os.path.join(root, file))

        if not all_files:
            raise Exception(f"Nenhum arquivo .parquet encontrado na pasta {self.parquet_folder} ou subpastas.")

        df_list = []
        for file in all_files:
            logger.info(f"Lendo arquivo: {file}")
            df = pd.read_parquet(file)
            df_list.append(df)

        combined_df = pd.concat(df_list, ignore_index=True)
        return combined_df

    def transform_data(self, df):
        """Aplica transformações nos dados antes de carregar no banco."""
        # Converte os nomes das colunas para minúsculas
        df.columns = [col.lower() for col in df.columns]

        # Aplica o tipo de dado correto de acordo com o schema
        for column in df.columns:
            if column in SCHEMA[self.table_name]:
                expected_type = SCHEMA[self.table_name][column]
                # Converte o tipo de dado da coluna
                if expected_type == Integer:
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                elif expected_type == String:
                    df[column] = df[column].astype(str)
                elif expected_type == DateTime:
                    df[column] = pd.to_datetime(df[column], errors='coerce')

        # Adiciona a coluna de data de carga
        df['data_carga'] = datetime.now()

        return df

    def load_to_postgres(self, df):
        """Carrega o DataFrame no banco de dados Postgres."""
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

            # Prepara a consulta de inserção com placeholders para cada coluna
            columns = df.columns.tolist()
            placeholders = ', '.join(['%s'] * len(columns))
            insert_query = f"""
                INSERT INTO {self.table_name} ({', '.join(columns)}) VALUES ({placeholders})
                """

            # Converte os dados para uma lista de tuplas
            data = [tuple(row) for row in df.values.tolist()]

            # Realiza a inserção em lotes
            execute_batch(cursor, insert_query, data, page_size=1000)
            conn.commit()

            logger.info(f"Dados carregados com sucesso na tabela {self.table_name}")
        except Exception as e:
            logger.error(f"Erro ao carregar os dados na tabela {self.table_name}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def run(self):
        """Executa o pipeline completo: leitura, transformação e carga."""
        try:
            df = self.read_parquet_files()
            logger.info("Arquivos .parquet lidos com sucesso.")
            df_transformed = self.transform_data(df)
            logger.info("Transformação dos dados concluída.")
            self.load_to_postgres(df_transformed)
            logger.info("Dados carregados no banco de dados com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao executar o pipeline para a tabela {self.table_name}: {e}")
            raise
