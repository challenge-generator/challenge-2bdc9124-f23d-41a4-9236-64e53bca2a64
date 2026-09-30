import logging
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
import json

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger(__name__)


class RedshiftWriter:
    """Escritor de datos transformados en Amazon Redshift con técnicas de batch y particionamiento."""

    def __init__(
        self,
        redshift_config: Dict[str, Any],
        batch_size: int = 1000,
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ):
        self.redshift_config = redshift_config
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.cluster_identifier = redshift_config.get("cluster_identifier")
        self.database = redshift_config.get("database", "dev")
        self.schema = redshift_config.get("schema", "public")
        self.iam_role = redshift_config_config.get("iam_role")
        self.s3_bucket = redshift_config.get("s3_bucket")
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client("redshift-data", region_name=self.redshift_config.get("region", "us-east-1"))
        return self._client

    def _generate_manifest_id(self) -> str:
        return f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"

    def _generate_s3_key(self, table_name: str, partition_date: str) -> str:
        return f"redshift/staging/{table_name}/{partition_date}/{self._generate_manifest_id()}/"

    def _execute_with_retry(self, operation, *args, **kwargs):
        last_exception = None
        for attempt in range(self.max_retries):
            try:
                return operation(*args, **kwargs)
            except (ClientError, BotoCoreError) as e:
                last_exception = e
                wait_time = self.retry_delay * (2 ** attempt)
                logger.warning(
                    f"Intento {attempt + 1}/{self.max_retries} falló para {operation.__name__}: {e}. "
                    f"Reintentando en {wait_time}s..."
                )
                time.sleep(wait_time)
        logger.error(f"Todos los intentos fallaron para {operation.__name__}")
        raise last_exception

    def write_batch(
        self,
        df: pd.DataFrame,
        table_name: str,
        partition_date: Optional[str] = None,
        mode: str = "append",
    ) -> Dict[str, Any]:
        if df.empty:
            logger.warning(f"DataFrame vacío, omitiendo escritura en {table_name}")
            return {"status": "skipped", "reason": "empty_dataframe"}

        partition_date = partition_date or datetime.utcnow().strftime("%Y-%m-%d")
        s3_key = self._generate_s3_key(table_name, partition_date)
        s3_path = f"s3://{self.s3_bucket}/{s3_key}"

        try:
            self._upload_to_s3_staging(df, s3_path)
            manifest_path = self._create_manifest(s3_key, table_name)
            self._execute_copy_command(table_name, manifest_path, mode)

            logger.info(f"Carga exitosa a {table_name} con {len(df)} registros")
            return {
                "status": "success",
                "table": table_name,
                "records": len(df),
                "s3_path": s3_path,
                "partition_date": partition_date,
            }
        except Exception as e:
            logger.error(f"Error en carga a Redshift: {e}")
            raise

    def _upload_to_s3_staging(self, df: pd.DataFrame, s3_path: str) -> None:
        parquet_buffer = pa.BufferOutputStream()
        table = pa.Table.from_pandas(df)
        pq.write_table(
            table,
            parquet_buffer,
            compression="snappy",
            use_dictionary=True,
            write_statistics=True,
        )
        parquet_bytes = parquet_buffer.getvalue().to_pybytes()

        s3_client = boto3.client("s3", region_name=self.redshift_config.get("region", "us-east-1"))
        s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=f"{s3_path}data.parquet",
            Body=parquet_bytes,
            ContentType="application/octet-stream",
        )
        logger.info(f"Subido parquet a {s3_path}data.parquet")

    def _create_manifest(self, s3_key: str, table_name: str) -> str:
        manifest_content = {
            "entries": [
                {"url": f"s3://{self.s3_bucket}/{s3_key}data.parquet", "mandatory": True}
            ]
        }
        manifest_key = f"{s3_key}manifest.json"
        s3_client = boto3.client("s3", region_name=self.redshift_config.get("region", "us-east-1"))
        s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=manifest_key,
            Body=json.dumps(manifest_content),
            ContentType="application/json",
        )
        return f"s3://{self.s3_bucket}/{manifest_key}"

    def _execute_copy_command(self, table_name: str, manifest_path: str, mode: str) -> None:
        copy_sql = f"""
        COPY {self.schema}.{table_name}
        FROM '{manifest_path}'
        IAM_ROLE '{self.iam_role}'
        FORMAT AS PARQUET
        { 'TRUNCATECOLUMNS' if mode == 'overwrite' else ''}
        """.strip()

        def execute():
            response = self.client.execute_statement(
                Database=self.database,
                Sql=copy_sql,
                ClusterIdentifier=self.cluster_identifier,
            )
            query_id = response.get("Id")
            self._wait_for_query_completion(query_id)
            return response

        self._execute_with_retry(execute)

    def _wait_for_query_completion(self, query_id: str, timeout: int = 300) -> None:
        start_time = time.time()
        while time.time() - start_time < timeout:
            describe_response = self.client.describe_statement(Id=query_id)
            status = describe_response.get("Status")
            if status in ["FINISHED"]:
                logger.info(f"Query {query_id} completada exitosamente")
                return
            elif status in ["FAILED", "ABORTED"]:
                error = describe_response.get("Error", "Error desconocido")
                raise RuntimeError(f"Query {query_id} falló: {error}")
            time.sleep(2)
        raise TimeoutError(f"Query {query_id} excedió el timeout de {timeout}s")

    def validate_table_exists(self, table_name: str) -> bool:
        check_sql = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{self.schema}' 
        AND table_name = '{table_name}'
        """.strip()

        try:
            response = self.client.execute_statement(
                Database=self.database,
                Sql=check_sql,
                ClusterIdentifier=self.cluster_identifier,
            )
            query_id = response.get("Id")
            self._wait_for_query_completion(query_id)
            result = self.client.get_statement_result(Id=query_id)
            return len(result.get("Records", [])) > 0
        except Exception as e:
            logger.error(f"Error validando existencia de tabla {table_name}: {e}")
            return False

    def get_table_metadata(self, table_name: str) -> Optional[Dict[str, Any]]:
        if not self.validate_table_exists(table_name):
            return None

        columns_sql = f"""
        SELECT column_name, data_type, is_nullable 
        FROM information_schema.columns 
        WHERE table_schema = '{self.schema}' 
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """.strip()

        try:
            response = self.client.execute_statement(
                Database=self.database,
                Sql=columns_sql,
                ClusterIdentifier=self.cluster_identifier,
            )
            query_id = response.get("Id")
            self._wait_for_query_completion(query_id)
            result = self.client.get_statement_result(Id=query_id)

            columns = [
                {
                    "name": row[0]["stringValue"],
                    "type": row[1]["stringValue"],
                    "nullable": row[2]["stringValue"] == "YES",
                }
                for row in result.get("Records", [])
            ]
            return {"table": table_name, "schema": self.schema, "columns": columns}
        except Exception as e:
            logger.error(f"Error obteniendo metadatos de {table_name}: {e}")
            return None


class RedshiftWriterFactory:
    @staticmethod
    def create(config: Dict[str, Any], batch_size: int = 1000) -> RedshiftWriter:
        return RedshiftWriter(
            redshift_config=config,
            batch_size=batch_size,
            max_retries=config.get("max_retries", 3),
            retry_delay=config.get("retry_delay", 2.0),
        )