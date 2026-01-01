import os
import io
import pandas as pd
import datetime
import logging

from typing import Optional, List, Dict
from pathlib import Path

from src.data_source.generic_data_source import GenericDataSource
from src.schema.schema_validation import (
    OrderSchema,
    OrderItemSchema,
    OrderItemRefundSchema,
    ProductSchema,
    WebsiteSessionsSchema,
    WebsitePageviewSchema
)
from src.cloud.cloud_connection import AzureCloud
from src.database.db_connection import DBConnection

logger = logging.getLogger(__name__)

class CSVDataSource(GenericDataSource):
    """Classe responsável por fazer a Coleta de Dados de arquivo do tipo CSV."""

    def __init__(
            self,
            default_path: Optional[str] = None,
            download_path: Optional[str] = None,
            azure_cloud: Optional[AzureCloud] = None,
            db_conn: Optional[DBConnection] = None
        ):
        """Inicializa a classe CSVDataSource."""
        super().__init__()

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        self.default_path = None
        self.download_path = None
        self.azure_cloud = azure_cloud or AzureCloud()
        self.db_conn = db_conn or DBConnection()

        if not default_path or default_path is None:
            self.default_path = 'src/docs/data'
        else:
            self.default_path = default_path

        if not download_path or download_path is None:
            self.download_path = 'src/docs/temp_downloads'
        else:
            self.download_path = download_path

        self.validation_schema = {
            'orders': OrderSchema,
            'order_items': OrderItemSchema,
            'order_item_refunds': OrderItemRefundSchema,
            'products': ProductSchema,
            'website_sessions': WebsiteSessionsSchema,
            'website_pageviews': WebsitePageviewSchema
        }

    def start(self):
        """Inicia a Pipeline de Dados."""
        start_time = datetime.datetime.now()

        self.db_conn.drop_tables()
        files_list = self.get_data()
        df_dict = self.transform_data(files_list)
        df_validado = self.validate_data(df_dict)
        self.load_data(df_validado)
        self.get_data_from_cloud()
        self.db_conn.create_tables()
        self.insert_data_into_db(df_validado)

        end_time = datetime.datetime.now()
        pipeline_time = (end_time - start_time).total_seconds()
        formated_time = pipeline_time / 60

        logger.info(f'Pipeline concluído em: {formated_time:.2f}min.')

    def get_data(self)  -> List[Path]:
        """Faz a coleta de arquivos do tipo CSV.
        
        Returns:
            List[Path]: Lista com os diretórios dos arquivos (ex: 'src/docs/data/orders')
        """
        logger.info('Iniciando Coleta de Dados...')

        files = []
        try:
            file_path = os.listdir(self.default_path)
            for file in file_path:
                if file.endswith('.csv'):
                    full_path = os.path.join(self.default_path, file)
                    files.append(full_path)

            logger.info(f'{len(files)} arquivo(s) coletado(s) com sucesso.')
            return files
        
        except Exception as e:
            logger.error(f'Erro ao coletar dados: {str(e)}')
            return []

    def transform_data(self, files_list: List[Path]) -> Dict[str, pd.DataFrame]:
        """Transforma os arquivos em um dicionário com {'nome do arquivo': DataFrame}.
        
        Args:
            files_list: Lista com os diretórios dos arquivos (ex: 'src/docs/data/orders').

        Returns:
            Dict(str, DataFrame): Dicionário com {'nome do arquivo': DataFrame}.
        """
        logger.info('Iniciando Transformação de Dados...')

        if not files_list or files_list is None:
            logger.warning('Transformação de arquivos cancelada. Nenhum arquivo foi passado.')
            return {}

        df_dict = {}
        try:
            for file in files_list:
                file_name = Path(file).stem
                df = pd.read_csv(file)
                df_dict[file_name] = df

                logger.info(f'{file_name}: {len(df)} linhas, {len(df.columns)} colunas.')

            logger.info(f'{len(df_dict)} arquivo(s) transformado(s) com sucesso.')
            return df_dict
        
        except Exception as e:
            logger.error(f'Erro ao transformar arquivos: {str(e)}')
            return {}
        
    def validate_data(self, df_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Faz a validação de Dados com Pandera.
        
        Args:
            df_dict (Dict[str, DataFrame]): Dicionário com {'nome do arquivo': pd.DataFrame}.

        Returns:
            Dict(str, pd.DataFrame): Dicionário com {'nome do arquivo': DataFrame validado}.
        """
        logger.info('Iniciando validação de Dados...')

        if not df_dict or df_dict is None:
            logger.warning('Validação cancelada. Nenhum arquivo foi passado.')
            return {}

        df_validate = {}
        try:
            for name, df in df_dict.items():
                validate_schema = self.validation_schema.get(name)
                df_validado = validate_schema.validate(df, lazy=True)
                df_validate[name] = df_validado

                logger.info(f'{name} validado.' )

            logger.info(f'{len(df_validate)} arquivo(s) validado(s).')
            return df_validate

        except Exception as e:
            logger.error(f'Erro ao validar dados: {str(e)}')
            return {}

    def load_data(self, df_dict: Dict[str, pd.DataFrame]) -> str:
        """Faz o Upload de Arquivos para a Azure.
        
        Args:
            df_dict (Dict[str, DataFrame]): Dicionário com {'nome do arquivo': DataFrame}.

        Returns:
            str: Mensagem de sucesso, se erro, mensagem de erro.
        """
        logger.info('Preparando arquivos para Upload...')

        try:
            for name, df in df_dict.items():
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
                parquet_data = parquet_buffer.getvalue()

                blob_name = f'raw_data/{name}.parquet'
                self.azure_cloud.upload_data(blob_name, parquet_data)

                logger.info(f'Upload concluído para: {blob_name}')

            logger.info(f'{len(df_dict)} arquivo(s) salvo(s) com sucesso.')

        except Exception as e:
            logger.error(f'Erro ao preparar arquivos: {str(e)}')
            raise

    def get_data_from_cloud(self) -> List[Path]:
        logger.info('Fazendo Download de Dados da Azure...')

        file_path = []
        try:
            blob_files = self.azure_cloud.list_blobs_file()
            parquet_files = [blob for blob in blob_files if blob.endswith('.parquet')]

            if not parquet_files:
                logger.warning('Nenhum arquivo parquet encontrado')
                return []
            
            temp_dir = Path(self.download_path)
            temp_dir.mkdir(exist_ok=True)

            for blob in parquet_files:
                data = self.azure_cloud.download_data(blob)
                file_name = Path(blob).name
                full_path = temp_dir / file_name

                with open(full_path, 'wb') as file:
                    file.write(data)

                logger.info(f'Arquivo: {blob} salvo com sucesso.')
                file_path.append(full_path)

            logger.info(f'{len(parquet_files)} arquivo(s) baixado(s) com sucesso.')
            return file_path
        
        except Exception as e:
            logger.error(f'Erro ao baixar arquivo: {str(e)}')
            return []
        
    def insert_data_into_db(self, df_dict: Dict[str, pd.DataFrame]):
        logger.info('Inserindo Dados...')

        try:
            self.db_conn.insert_data(df_dict)
            logger.info('Inserção de Dados concluida com sucesso.')

        except Exception as e:
            logger.error(f'Erro ao inserir dados no Banco de Dados: {str(e)}')
            raise

if __name__ == '__main__':
    data_source = CSVDataSource().start()