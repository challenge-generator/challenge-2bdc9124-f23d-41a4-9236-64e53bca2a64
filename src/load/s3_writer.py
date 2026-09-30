import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
import json
import os

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger(__name__)


class S3Writer:
    """Escritor de datos crudos y transformados en AWS S3 con formato columnar (Parquet)."""

    def __init__(
        self,
        bucket_name: str,
        region: str = "us-east-1",
        compression: str = "snappy",
        use_dictionary: bool = True,
        write_statistics: bool = True,
    ):
        self.bucket_name = bucket_name
        self.region = region
        self.compression = compression
        self.use_dictionary = use_dictionary
        self.write_statistics = write_statistics
        self._client = None
        self._partition_keys = ["year", "month", "day", "hour"]

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client("s3", region_name=self.region)
        return self._client

    def _generate_partition_path(
        self,
        timestamp: Optional[datetime] = None,
        partition_values: Optional[Dict[str, str]] = None,
    ) -> str:
        if partition_values:
            return "/".join([f"{k}={v}" for k, v in partition_values.items()])

        ts = timestamp or datetime.utcnow()
        return f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/hour={ts.hour:02d}"

    def _generate_run_id(self) -> str:
        return f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"

    def write_raw(
        self,
        data: Any,
        prefix: str,
        format: str = "parquet",
        partition_timestamp: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        partition_path = self._generate_partition_path(partition_timestamp)
        run_id = self._generate_run_id()
        key = f"raw/{prefix}/{partition_path}/{run_id}.{format}"

        try:
            if isinstance(data, pd.DataFrame):
                self._write_parquet(data, key)
            elif isinstance(data, (dict, list)):
                self._write_json(data, key)
            elif isinstance(data, bytes):
                self._write_raw_bytes(data, key)
            else:
                raise ValueError(f"Tipo de datos no soportado: {type(data)}")

            if metadata:
                self._write_metadata(key, metadata)

            full_path = f"s3://{self.bucket_name}/{key}"
            logger.info(f"Datos crudos escritos en {full_path}")
            return {
                "status": "success",
                "path": full_path,
                "key": key,
                "partition": partition_path,
                "run_id": run_id,
            }
        except Exception as e:
            logger.error(f"Error escribiendo datos crudos: {e}")
            raise

    def write_transformed(
        self,
        df: pd.DataFrame,
        dataset_name: str,
        partition_timestamp: Optional[datetime] = None,
        partition_keys: Optional[List[str]] = None,
        mode: str = "overwrite",
    ) -> Dict[str, Any]:
        if df.empty:
            logger.warning(f"DataFrame vacío para dataset {dataset_name}, omitiendo escritura")
            return {"status": "skipped", "reason": "empty_dataframe"}

        partition_path = self._generate_partition_path(partition_timestamp)
        partition_cols = partition_keys or self._partition_keys
        run_id = self._generate_run_id()

        df_with_partition = df.copy()
        ts = partition_timestamp or datetime.utcnow()
        df_with_partition["year"] = ts.year
        df_with_partition["month"] = ts.month
        df_with_partition["day"] = ts.day
        df_with_partition["hour"] = ts.hour
        df_with_partition["_processed_at"] = ts.isoformat()
        df_with_partition["_run_id"] = run_id

        key = f"transformed/{dataset_name}/{partition_path}/{run_id}.parquet"

        try:
            self._write_parquet(
                df_with_partition,
                key,
                partition_cols=partition_cols,
            )

            self._update_latest_marker(dataset_name, key)

            full_path = f"s3://{self.bucket_name}/{key}"
            logger.info(f"Datos transformados escritos en {full_path}")
            return {
                "status": "success",
                "path": full_path,
                "key": key,
                "dataset": dataset_name,
                "records": len(df),
                "partition": partition_path,
                "run_id": run_id,
            }
        except Exception as e:
            logger.error(f"Error escribiendo datos transformados: {e}")
            raise

    def _write_parquet(
        self,
        df: pd.DataFrame,
        key: str,
        partition_cols: Optional[List[str]] = None,
    ) -> None:
        table = pa.Table.from_pandas(df, preserve_index=False)

        parquet_buffer = pa.BufferOutputStream()
        pq.write_table(
            table,
            parquet_buffer,
            compression=self.compression,
            use_dictionary=self.use_dictionary,
            write_statistics=self.write_statistics,
            version="2.6",
        )

        parquet_bytes = parquet_buffer.getvalue().to_pybytes()

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=parquet_bytes,
            ContentType="application/octet-stream",
            Metadata={
                "record_count": str(len(df)),
                "schema": json.dumps([str(f) for f in df.columns.tolist()]),
            },
        )
        logger.info(f"Parquet escrito: {key} ({len(df)} registros)")

    def _write_json(self, data: Any, key: str) -> None:
        if isinstance(data, list):
            content = "\n".join([json.dumps(item, default=str) for item in data])
            content_type = "application/x-ndjson"
        else:
            content = json.dumps(data, default=str)
            content_type = "application/json"

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType=content_type,
        )
        logger.info(f"JSON escrito: {key}")

    def _write_raw_bytes(self, data: bytes, key: str) -> None:
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=data,
            ContentType="application/octet-stream",
        )
        logger.info(f"Bytes crudos escritos: {key}")

    def _write_metadata(self, data_key: str, metadata: Dict[str, Any]) -> None:
        metadata_key = data_key.replace(".parquet", "_metadata.json").replace(".json", "_metadata.json")
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info(f"Metadatos escritos: {metadata_key}")

    def _update_latest_marker(self, dataset_name: str, key: str) -> None:
        marker_key = f"transformed/{dataset_name}/_latest"
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=marker_key,
            Body=key.encode("utf-8"),
            ContentType="text/plain",
        )

    def list_partitions(
        self,
        dataset_name: str,
        prefix: str = "transformed",
    ) -> List[Dict[str, Any]]:
        prefix_path = f"{prefix}/{dataset_name}/"
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix_path,
                Delimiter="/",
            )

            partitions = []
            for common_prefix in response.get("CommonPrefixes", []):
                partition_key = common_prefix.get("Prefix", "")
                partitions.append({
                    "path": f"s3://{self.bucket_name}/{partition_key}",
                    "key": partition_key,
                })
            return partitions
        except ClientError as e:
            logger.error(f"Error listando particiones: {e}")
            return []

    def read_latest(self, dataset_name: str) -> Optional[pd.DataFrame]:
        marker_key = f"transformed/{dataset_name}/_latest"
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=marker_key)
            latest_key = response["Body"].read().decode("utf-8").strip()

            if not latest_key:
                return None

            response = self.client.get_object(Bucket=self.bucket_name, Key=latest_key)
            parquet_bytes = response["Body"].read()

            table = pq.read_table(pa.py_buffer(parquet_bytes))
            return table.to_pandas()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.warning(f"No existe versión latest para {dataset_name}")
                return None
            logger.error(f"Error leyendo latest de {dataset_name}: {e}")
            return None


class S3WriterFactory:
    @staticmethod
    def create(config: Dict[str, Any]) -> S3Writer:
        return S3Writer(
            bucket_name=config["bucket"],
            region=config.get("region", "us-east-1"),
            compression=config.get("compression", "snappy"),
            use_dictionary=config.get("use_dictionary", True),
            write_statistics=config.get("write_statistics", True),
        )