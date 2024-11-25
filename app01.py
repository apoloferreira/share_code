import os
import boto3
import duckdb
from pathlib import Path
from typing import Iterable, Tuple
from botocore.exceptions import ClientError
from duckdb import DuckDBPyConnection


DB_NAME = "catalog.db"
BUCKET = "mlops-us-east-1-891377318910"
PREFIX_DB = f"duck_catalog/{DB_NAME}"
# PATH_DB = os.path.join(os.getcwd(), DB_NAME)
# PATH_DB = Path.cwd() / DB_NAME
PATH_DB = Path(DB_NAME)

create_view_template = """
CREATE OR REPLACE VIEW {schema}.{table_name} AS
SELECT * FROM read_parquet(
'{location}',
hive_partitioning={partitioned}
);
""".strip()


client_glue = boto3.client(service_name="glue")
client_s3 = boto3.client(service_name="s3")

def glue_databases() -> list:
    resp_db = client_glue.get_databases(MaxResults=500)
    return [
        {'name': item['Name'], 'account': item['CatalogId']}
        for item in resp_db['DatabaseList']
    ]

def glue_tables(databases: list) -> list:
    tables = []
    for db in databases:
        db_name = db['name']
        if db_name != "base":
            continue
        resp_tb = client_glue.get_tables(DatabaseName=db_name)
        for tb in resp_tb['TableList']:
            tables.append({
                'name': (tb_name := tb.get('Name')),
                'database': (dd_name := tb.get('DatabaseName')),
                'id': f'{dd_name}.{tb_name}',
                'partitions': tb.get('PartitionKeys'),
                'location': tb.get('StorageDescriptor', {}).get('Location'),
                'is_compressed': tb.get('StorageDescriptor', {}).get('Compressed'),
                'compression': tb.get('Parameters', {}).get('compressionType'),
                'classification': tb.get('Parameters', {}).get('classification'),
            })
    return tables

def download_s3(bucket_name: str, key: str, local_path: str) -> None:
    try:
        client_s3.download_file(Bucket=bucket_name, Key=key, Filename=local_path)
    except ClientError:
        print("No existing data in S3 to copy")

def upload_s3(bucket_name: str,  key: str, local_path: Path) -> None:
    try:
        client_s3.upload_file(
            Filename=str(local_path),
            Bucket=bucket_name,
            Key=key
        )
    except ClientError:
        print("No existing data in Local to copy")

def parse_s3_url(s3_url: str) -> tuple[str, str]:
    parts = s3_url[5:].split('/', 1)
    bucket_name = parts[0]
    prefix = parts[1] if len(parts) > 1 else ''
    return bucket_name, prefix.rstrip('/')

def find_s3_objects(s3_url: str) -> list[str]:
    bucket_name, prefix = parse_s3_url(s3_url)
    paginator = client_s3.get_paginator('list_objects_v2')
    len_prefix = len(prefix.split("/"))
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                path = Path(key)
                if path.suffix == '.parquet':
                    len_key = ["*"] * (len(key.split("/")) - len_prefix)
                    return f"s3://{bucket_name}/{prefix}/{'/'.join(len_key)}.parquet"


class DuckDBSession:

    def __init__(self, db_path: Path, remote_tables: list):
        self.db_path = db_path
        self.remote_tables = remote_tables
        self.conn = self.create_connection()

    def database_exist(self) -> bool:
        return Path.exists(self.db_path)

    def connection_prereq(self, conn: DuckDBPyConnection) -> DuckDBPyConnection:
        if not self.database_exist():
            print("DB nao existe")
            conn.sql("INSTALL parquet;")
            conn.sql("INSTALL httpfs;")
            conn.sql("INSTALL aws;")
            conn = self.create_schema(conn)
        conn.sql("LOAD parquet;")
        conn.sql("LOAD httpfs;")
        conn.sql("LOAD aws;")
        conn.sql("CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN);")
        return conn

    def create_schema(self, conn: DuckDBPyConnection) -> DuckDBPyConnection:
        for db in set([item['database'] for item in self.remote_tables]):
            conn.sql(f"CREATE SCHEMA {db};")
        return conn

    def create_table(self, tables: list):
        for item in tables:
            print(f"Criando View: {item['name']}.{item['database']}")
            obj_location = find_s3_objects(item['location'])
            create_view = create_view_template.format_map({
                'schema': item['database'],
                'table_name': item['name'],
                'location': obj_location,
                'partitioned': 'true' if item['partitions'] else 'false',
            })
            self.conn.sql(create_view)

    def create_connection(self) -> DuckDBPyConnection:
        conn = duckdb.connect(database=self.db_path, read_only=False)
        conn = self.connection_prereq(conn)
        return conn

    def delete_table(self, to_delete: list) -> None:
        for item in to_delete:
            print(f"Deletando View: {item}")
            self.conn.sql(f"DROP VIEW IF EXISTS {item};")

    def update_database(self, to_create: list, to_delete: list) -> None:
        print("Atualizando database")
        self.delete_table(to_delete)
        create_formatted = filter(lambda x: x['id'] in to_create, self.remote_tables)
        self.create_table(create_formatted)

    def show_tables(self):
        self.conn.sql("SHOW all tables;").show()

    def get_local_tables(self) -> list:
        tables = self.conn.sql("SHOW all tables;").fetchall()
        return [f"{item[1]}.{item[2]}" for item in tables]

    def get_remote_tables(self) -> list:
        return [f"{item['database']}.{item['name']}" for item in self.remote_tables]

    def compare_local_with_remote(self) -> Tuple[list, list]:
        print("Comparando tables locais com remotas")
        local_tables = self.get_local_tables()
        remote_tables = self.get_remote_tables()
        to_create = list(filter(lambda x: x not in local_tables, remote_tables))
        to_delete = list(filter(lambda x: x not in remote_tables, local_tables))
        # print("To Create", to_create)
        # print("To Delete:", to_delete)
        return (to_create, to_delete)

    def refresh(self):
        to_create, to_delete = self.compare_local_with_remote()
        self.update_database(to_create, to_delete)
        return self

    def get_conn(self) -> DuckDBPyConnection:
        return self.conn


if __name__ == "__main__":
    download_s3(BUCKET, PREFIX_DB, PATH_DB)
    databases = glue_databases()
    tables = glue_tables(databases)

    # for item in tables:
    #     print("-"*100)
    #     print(item)
    #     print("-"*100)
    # print("="*100)

    duckdb_session = DuckDBSession(PATH_DB, tables)
    duckdb_session.refresh()
    duckdb_session.show_tables()

    upload_s3(BUCKET, PREFIX_DB, PATH_DB)
