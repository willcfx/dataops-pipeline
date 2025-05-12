import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import json
import logging
from tools.pipeline import Pipeline
from tools.schema import SCHEMA
import logging
import psycopg2
from psycopg2.extras import execute_batch
# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoldComercioExteriorLoader:

    def __init__(self, parquet_folder=Pipeline.STORAGE_EXP, table_name=Pipeline.TABLE_F_COMEX_ESTADOS, json_path=Pipeline.JSON_PATH):
        self.parquet_folder = parquet_folder
        self.table_name = table_name
        self.json_path = json_path

        # Carrega a configuração do banco de dados
        self.db_config = self.load_db_config()

        # Chama o método para criar a engine do SQLAlchemy
        self.engine = self.create_sqlalchemy_engine()

        # Realiza a conexão para garantir que está tudo certo
        self.test_connection()

    def load_db_config(self):
        
        try:
            with open(self.json_path, 'r') as f:
                db_config = json.load(f)
            return db_config[0]
        except Exception as e:
            raise RuntimeError(f"Erro ao carregar config do banco: {e}")
     
    def test_connection(self):
        """Testa a conexão com o banco de dados."""
        try:
            with self.engine.connect() as conn:
                logger.info("Conexão com o banco de dados bem-sucedida.")
        except Exception as e:
            logger.error(f"Erro ao conectar ao banco de dados: {e}")
            raise
    
    def create_sqlalchemy_engine(self):
        try:
            conn_str = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            return create_engine(conn_str)
        except Exception as e:
            raise RuntimeError(f"Erro ao criar SQLAlchemy engine: {e}")

    def transform_data(self):
        try:
            with self.engine.connect() as conn:
                query_exp = """
                    SELECT
                        CO_ANO,
                        CO_MES,
                        CO_NCM,
                        CO_PAIS,
                        SG_UF_NCM,
                        CO_VIA,
                        CO_URF,
                        VL_FOB,
                        'EXP' AS TIPO_MOV
                    FROM SILVER.F_EXPORTACOES_ESTADOS
                """

                query_imp = """
                    SELECT
                        CO_ANO,
                        CO_MES,
                        CO_NCM,
                        CO_PAIS,
                        SG_UF_NCM,
                        CO_VIA,
                        CO_URF,
                        VL_FOB,
                        'IMP' AS TIPO_MOV
                    FROM SILVER.F_IMPORTACOES_ESTADOS
                """

                df_exp = pd.read_sql(query_exp, conn)
                df_imp = pd.read_sql(query_imp, conn)

                df_gold = pd.concat([df_exp, df_imp], ignore_index=True)
                return df_gold

        except Exception as e:
            raise RuntimeError(f"Erro ao extrair e unir dados SILVER: {e}")

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
            
            df_transformed = self.transform_data()
            logger.info("Transformação dos dados concluída.")
            self.load_to_postgres(df_transformed)
            logger.info("Dados carregados no banco de dados com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao executar o pipeline para a tabela {self.table_name}: {e}")
            raise
       
