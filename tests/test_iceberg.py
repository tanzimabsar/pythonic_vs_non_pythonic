"""
Using pyiceberg as the underling storage mechanism,
represent a table as a dictionary or mutable mapping
"""
import uuid
from collections import abc
from datetime import datetime

from pyiceberg.catalog.sql import SqlCatalog
import pyarrow as pa
import pyiceberg
from contextlib import suppress
import logging
from pydantic import Field, BaseModel

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SCHEMA = pa.schema([
    pa.field('id', pa.string()),
    pa.field('batch_id', pa.string()),
    pa.field('created_at', pa.timestamp('ms')),
    pa.field('status', pa.string())
])

class StateStore:
    def __init__(self, namespace: str, catalog: SqlCatalog, **kwargs):
        self.namespace = namespace
        self.catalog = catalog
        self.table = kwargs.get('table', None)

        self.warehouse_path = kwargs['warehouse_path'] or "./data/warehouse"

        if not self.namespace:
            self.namespace = "default"

        if not self.catalog:
            uri = f"sqlite:///{self.warehouse_path}/pyiceberg_catalog.db"
            warehouse = f"file://{self.warehouse_path}"
            self.catalog = SqlCatalog(self.namespace, **{"uri": uri, "warehouse": warehouse})

        with suppress(pyiceberg.exceptions.NamespaceAlreadyExistsError):
            logger.warning('Namespace already exists!')
            self.catalog.create_namespace(self.namespace)

    def add(self, table, schema):
        with suppress(pyiceberg.exceptions.TableAlreadyExistsError):
            logger.info('Already exists!')
            self.catalog.create_table(f'{self.namespace}.{table}', schema=schema)

        return self.catalog.load_table(f'{self.namespace}.{table}')


class Request(BaseModel):
    id: str = Field(default=str(uuid.uuid4()))
    batch_id: str = Field(default=str(uuid.uuid4()))
    created_at: datetime = Field(default=datetime.now())
    status: str = Field(default='pending')

def test_access_state_store():
    store = StateStore(namespace=None, catalog=None, warehouse_path="./data/warehouse")
    table = store.add(table='requests', schema=SCHEMA)

    my_record = Request()
    df = pa.Table.from_pylist([my_record.model_dump()])
    table.append(df)
    print(table.scan().to_pandas())



def test_create_if_not_exists():
    store = StateStore(namespace=None, catalog=None, warehouse_path="./data/warehouse")
    store.add(table='users', schema=pa.schema([
        pa.field('id', pa.int64()),
        pa.field('name', pa.string())
    ]))

def test_use_database_with_sql():
    store = StateStore(namespace=None, catalog=None, warehouse_path="./data/warehouse")
    table = store.add(table='requests', schema=SCHEMA)

    conn = table.scan().to_duckdb(table_name='requests')

    df = pa.Table.from_pylist([Request().model_dump() for _ in range(94 * 1000)])
    table.append(df)
    print(conn.execute("SELECT * FROM requests where batch_id = '0591c741-1ffa-451d-aaf6-b062221e87fa'").fetchdf())

    """ 
    Identify what email belongs to which batch 
    Identify what email was obfuscated in which table
    
    """





