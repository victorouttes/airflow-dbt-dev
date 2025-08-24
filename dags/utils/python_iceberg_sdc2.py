import hashlib
import os
from typing import List

import pandas as pd
import pyarrow as pa
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from loguru import logger
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, In, EqualTo
from pyiceberg.table import Table


class IcebergDatabase:
    def __init__(self, database_name: str, catalog_s3_path: str, aws_hook: S3Hook = None):
        self.database = database_name
        self.iceberg_catalog = catalog_s3_path
        if aws_hook:
            self.aws_access_key = aws_hook.get_credentials().access_key
            self.aws_secret_key = aws_hook.get_credentials().secret_key
            self.aws_region = aws_hook.conn_region_name or 'us-east-1'
            os.environ['AWS_ACCESS_KEY_ID'] = self.aws_access_key
            os.environ['AWS_SECRET_ACCESS_KEY'] = self.aws_secret_key
            os.environ['AWS_DEFAULT_REGION'] = self.aws_region


class IcebergTable:
    def __init__(self, database: IcebergDatabase, table_name: str, business_keys: List[str]):
        self.__database = database.database
        self.__catalog = database.iceberg_catalog
        self.__table_name = table_name
        self.__business_keys = business_keys
        self.__control_columns = ['_hashkey', '_hashdata', '_valid_from', '_valid_to', '_is_current']
        self.__original_columns = None
        self.__pa_schema_original = []
        self.__pa_schema_control = []
        self.__full_schema = None
        self.__new_data = None

    def prepare_data(self, df: pd.DataFrame):
        logger.info(f'Preparing data for table {self.__table_name}...')
        df = df.astype(str)
        self.__original_columns = df.columns.tolist()
        logger.info(f'Original columns: {self.__original_columns}')
        logger.info(f'Business keys: {self.__business_keys}')
        logger.info(f'Data size to analyze: {len(df)}')
        self.__pa_schema_original = [pa.field(name=name, type=pa.string()) for name in self.__original_columns]
        self.__pa_schema_control = [
            pa.field(name='_hashkey', type=pa.string()),
            pa.field(name='_hashdata', type=pa.string()),
            pa.field(name='_valid_from', type=pa.timestamp('us')),
            pa.field(name='_valid_to', type=pa.timestamp('us')),
            pa.field(name='_is_current', type=pa.bool_())
        ]
        self.__full_schema = pa.schema(self.__pa_schema_original + self.__pa_schema_control)
        new_data = self.__add_control_columns(df)
        self.__new_data = pa.Table.from_pandas(df=new_data, schema=self.__full_schema)
        return self

    def scd2_add(self):
        table = self.__load_table()

        # pegar chaves dos dados novos
        keys = self.__new_data['_hashkey'].to_pylist()

        # buscar versoes atuais dessas chaves
        row_filter = And(In('_hashkey', keys), EqualTo('_is_current', True))
        existing_data = table.scan(row_filter=row_filter).to_arrow()
        existing_map = {r['_hashkey']: r for r in existing_data.to_pylist()}

        inserts = []
        overwrites = []

        # comparacao
        for i in range(self.__new_data.num_rows):
            record = {col: self.__new_data[col][i].as_py() for col in self.__new_data.column_names}
            key = record['_hashkey']
            data_hash = record['_hashdata']

            old = existing_map.get(key)
            if old is None:
                # dado novo
                inserts.append(record)
            else:
                # apenas se teve alguma alteracao
                if old['_hashdata'] != data_hash:
                    # fecha versao anterior
                    closed = dict(old)
                    closed['_is_current'] = False
                    closed['_valid_to'] = record['_valid_from']
                    overwrites.append(closed)
                    # adiciona nova versao
                    inserts.append(record)

        if not inserts and not overwrites:
            logger.info('Nothing to do with this data!')
        else:
            logger.info(f'Inserting {len(inserts)-len(overwrites)} records...')
            logger.info(f'Updating {len(overwrites)} records...')
            with table.transaction() as tx:
                if overwrites:
                    keys_to_update = [r['_hashkey'] for r in overwrites]
                    overwrite_filter = And(In('_hashkey', keys_to_update), EqualTo('_is_current', True))
                    df_over = pa.Table.from_pylist(overwrites, schema=self.__full_schema)
                    tx.overwrite(df=df_over, overwrite_filter=overwrite_filter)

                if inserts:
                    df_ins = pa.Table.from_pylist(inserts, schema=self.__full_schema)
                    tx.append(df=df_ins)

        del os.environ['AWS_ACCESS_KEY_ID']
        del os.environ['AWS_SECRET_ACCESS_KEY']
        del os.environ['AWS_DEFAULT_REGION']
        logger.info('Done!')

    def __add_control_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df['_hashkey'] = df[self.__business_keys].apply(
            lambda row: hashlib.sha256('|'.join(row.astype(str)).encode()).hexdigest(), axis=1)
        df['_hashdata'] = df[self.__original_columns].apply(
            lambda row: hashlib.sha256('|'.join(row.astype(str)).encode()).hexdigest(), axis=1)
        df['_valid_from'] = pd.Timestamp.now()
        df['_valid_to'] = None
        df['_is_current'] = True
        return df

    def __load_table(self) -> Table:
        glue = load_catalog(
            name='default',
            type='glue'
        )
        if glue.table_exists(f'{self.__database}.{self.__table_name}'):
            table = glue.load_table(f'{self.__database}.{self.__table_name}')
        else:
            table = glue.create_table(
                identifier=f'{self.__database}.{self.__table_name}',
                schema=self.__full_schema,
                location=f'{self.__catalog}/{self.__table_name}'
            )
        return table
