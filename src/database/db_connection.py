import os
import logging
import pandas as pd

from dotenv import load_dotenv
from typing import Optional, Dict

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.database.db_model import (
    Base,
    OrderTable,
    OrderItemTable,
    OrderItemRefundTable,
    ProductsTable,
    WebSiteSessionsTable,
    WebSitePageViewsTable
)

logger = logging.getLogger(__name__)

class DBConnection:
    """Classe responsável por fazer as conexões com o Banco de Dados."""

    def __init__(self):
        """Inicializa a classe DBConnection."""
        load_dotenv()

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        self.db_user = os.getenv('DB_USER')
        self.db_pass = os.getenv('DB_PASS')
        self.db_host = os.getenv('DB_HOST')
        self.db_port = os.getenv('DB_PORT')
        self.db_name = os.getenv('DB_NAME')

        try:
            self.engine = create_engine(
                f'postgresql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}',
                echo=False
            )

            self._Session = sessionmaker(bind=self.engine)
            self.Base = Base

        except Exception as e:
            logger.error(f'Erro ao se conectar ao Banco de Dados: {str(e)}')
            raise

        self.ORM_MAPPING = {
            'orders': OrderTable,
            'order_items': OrderItemTable,
            'order_item_refunds': OrderItemRefundTable,
            'products': ProductsTable,
            'website_sessions': WebSiteSessionsTable,
            'website_pageviews': WebSitePageViewsTable
        }

        self.pk_mapping = {
            'orders': 'order_id',
            'order_items': 'order_item_id',
            'order_item_refunds': 'order_item_refund_id',
            'products': 'product_id',
            'website_sessions': 'website_session_id',
            'website_pageviews': 'website_pageview_id'
        }

    def create_tables(self):
        """Cria as Tabelas do Banco de Dados."""
        logger.info('Criando Tabelas....')

        try:
            self.Base.metadata.create_all(self.engine)
            logger.info('Tabelas criadas com sucesso.')

        except Exception as e:
            logger.error(f'Erro ao criar as tabelas: {str(e)}')
            raise

    def drop_tables(self):
        """Deleta TODAS as Tabelas do Banco de Dados."""
        logger.warning('Deletando TODAS as Tabelas do Banco de Dados...')

        try:
            self.Base.metadata.drop_all(self.engine)
            logger.info('Tabelas deletadas com sucesso.')

        except Exception as e:
            logger.error(f'Erro ao deletar as tabelas: {str(e)}')
            raise

    def insert_data(self, df_dict: Dict[str, pd.DataFrame], batch_size: Optional[int] = 1000) -> str:
        """Insere Dados no Banco de Dados.
        
        Args:
            df_dict (Dict[str, DataFrame]): Dicionário com {'nome do arquivo': pd.DataFrame}.
            batch_size (Optional[int]): Tamanho do lote a ser inserido.

        Returns:
            str: Mensagem de sucesso, se erro, mensagem de erro.
        """
        logger.info('Inserindo Dados no Banco de Dados...')

        session = self._Session()

        try:
            for name, df in df_dict.items():
                model = self.ORM_MAPPING.get(name)
                records = df.to_dict(orient='records')
                total = len(records)

                for i in range(0, total, batch_size):
                    batch = records[i:i + batch_size]
                    session.bulk_insert_mappings(model, batch)
                    
                logger.info(f'{total} linhas inseridas em: {name}')

            session.commit()
            logger.info('Valores inseridos com sucesso.')

        except Exception as e:
            logger.error(f'Erro ao inserir dados: {str(e)}')
            session.rollback()
            raise

        finally:
            session.close()

    def update_data(self, df_dict: Dict[str, pd.DataFrame], batch_size: Optional[int] = 500) -> str:
        """Atualiza Dados no Banco de Dados.
        
        Args:
            df_dict (Dict[str, DataFrame]): Dicionário com {'nome do arquivo': pd.DataFrame}.
            batch_size (Optional[int]): Tamanho do lote a ser inserido.

        Returns:
            str: Mensagem de sucesso, se erro, mensagem de erro.
        """
        logger.info('Iniciando Update de Dados...')

        session = self._Session()

        try:
            for name, df in df_dict.items():
                model = self.ORM_MAPPING.get(name)
                records = df.to_dict(orient='records')
                total = len(records)

                for i in range(0, total, batch_size):
                    batch = records[i:i + batch_size]
                    session.bulk_update_mappings(model, batch)

                logger.info(f'{total} linhas atualizdas em: {name}')

            session.commit()
            logger.info('Tabelas atualizadas com sucesso.')

        except Exception as e:
            logger.error(f'Erro ao atualizar dados: {str(e)}')
            session.rollback()
            raise

        finally:
            session.close()

    def upsert_data(self, df_dict: Dict[str, pd.DataFrame]) -> str:
        """Faz Upsert de Dados no Banco de Dados (Atualiza e Insere Novos).
        
        Args:
            df_dict (Dict[str, DataFrame]): Dicionário com {'nome do arquivo': pd.DataFrame}.

        Returns:
            str: Mensagem de sucesso, se erro, mensagem de erro.
        """
        logger.info('Iniciando Upsert de Dados...')

        session = self._Session()

        try:
            for name, df in df_dict.items():
                model = self.ORM_MAPPING.get(name)
                pk_column = self.pk_mapping.get(name)

                records = df.to_dict(orient='records')
                for record in records:
                    pk_value = record.get(pk_column)
                    existing = session.query(model).filter(
                        getattr(model, pk_column) == pk_value
                    ).first()

                    if existing:
                        for key, value in record.items():
                            setattr(existing, key, value)
                    else:
                        obj = model(**record)
                        session.add(obj)

            session.commit()
            logger.info('Upsert concluíd com sucesso.')

        except Exception as e:
            logger.error(f'Erro ao fazer o upsert de dados: {str(e)}')
            session.rollback()
            raise

        finally:
            session.close()


    def incremental_load(self, df_dict: Dict[str, pd.DataFrame], batch_size: Optional[int] = 1000):
        """Faz atualização Incremental dos Dados na Tabela.
        
        Args:
            df_dict (Dict[str, DataFrame]): Dicionário com {'nome do arquivo': pd.DataFrame}.
            batch_size (Optional[int]): Tamanho do lote a ser inserido.

        Returns:
            str: Mensagem de sucesso, se erro, mensagem de erro.
        """
        logger.info('Iniciando Carga Incremental...')

        session = self._Session()

        try:
            for name, df in df_dict.items():
                model = self.ORM_MAPPING.get(name)
                pk_column = self.pk_mapping.get(name)

                existing_ids = set(
                    row[0] for row in session.query(getattr(model, pk_column)).all()
                )
                records = df.to_dict(orient='records')

                new_records = []
                for record in records:
                    pk_value = record.get(pk_column)

                    if pk_value in existing_ids:
                        pass
                    if pk_value not in existing_ids:
                        new_records.append(record)

                    if new_records:
                        for i in range(0, len(new_records), batch_size):
                            batch = new_records[i:i + batch_size]
                            session.bulk_insert_mappings(model, batch)

                logger.info(f'{len(new_records)} adicionados em: {name}')

            session.commit()
            logger.info('Atualização incremental concluída com sucesso.')

        except Exception as e:
            logger.error(f'Erro ao fazer a atualização incremental: {str(e)}')
            session.rollback()
            raise
        
        finally:
            session.close()