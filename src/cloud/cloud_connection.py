import os
import logging

from pathlib import Path
from dotenv import load_dotenv
from typing import List

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)

class AzureCloud:
    """Classe responsável por fazer as conexões com a Azure."""

    def __init__(self):
        """Inicializa a classe AzureCloud"""
        load_dotenv()

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        self.client_id = os.getenv('AZURE_CLIENT_ID')
        self.tenant_id = os.getenv('AZURE_TENANT_ID')
        self.client_secret = os.getenv('AZURE_CLIENT_SECRET')
        self.account_url = os.getenv('AZURE_STORAGE_URL')
        self.container_name = os.getenv('AZURE_CONTAINER_NAME')
        self.blob_prefix = os.getenv('AZURE_BLOB_PREFIX')

        try:
            self.credentials = ClientSecretCredential(
                client_id=self.client_id,
                tenant_id=self.tenant_id,
                client_secret=self.client_secret
            )
        
        except Exception as e:
            logger.error(f'Erro ao se conectar com a Azure: {str(e)}')
            raise

        try:
            self.blob_service_client = BlobServiceClient(
                account_url=self.account_url,
                credential=self.credentials
            )

        except Exception as e:
            logger.error(f'Erro ao se conectar com o Container: {str(e)}')
            raise

    def upload_data(self, blob_name: str, data: bytes) -> str:
        """Faz o Upload de Arquivos para a Azure.
        
        Args:
            blob_name (str): Nome do arquivo a ser salvo.
            data (bytes): Conteúdo binário do arquivo.

        Returns:
            str: Mensagem de sucesso, se erro, mensagem de erro.
        """
        logger.info('Salvando Dados na Azure...')

        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )

            blob_client.upload_blob(data, overwrite=True)
            logger.info(f'Arquivo {blob_name} salvo com sucesso.')

        except Exception as e:
            logger.error(f'Erro ao salvar dados: {str(e)}')
            raise
    
    def download_data(self, blob_name: str) -> bytes:
        """Faz o Download de Arquivos da Azure.
        
        Args:
            blob_name (str): Nome do arquivo a ser baixado.

        Returns:
            bytes: Conteúdo binário do arquivo.
        """
        logger.info('Fazendo Dowload de Arquivos...')

        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )

            download_buffer = blob_client.download_blob()
            data = download_buffer.readall()

            logger.info(f'Download concluído com sucesso para: {blob_name}')
            return data
        
        except Exception as e:
            logger.error(f'Erro ao baixar aquivo: {str(e)}')
            raise

    def list_blobs_file(self) -> List[Path]:
        """Lista arquivos no Container da Azure.
        
        Returns:
            List[Path]: Lista com os diretórios dos arquivos (ex:'raw_data/products').
        """
        logger.info('Listando arquivos...')

        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            blob_names = container_client.list_blobs(name_starts_with=self.blob_prefix)

            blobs = [blob.name for blob in blob_names]

            logger.info(f'{len(blobs)} arquivos listados.')
            return blobs
        
        except Exception as e:
            logger.error(f'Erro ao listar arquivos: {str(e)}')
            return []