"""
Lector de datos desde AWS S3 para fuentes batch integradas en el flujo streaming.
Maneja múltiples formatos, particionamiento y lectura eficiente.
"""
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional, Any
from io import BytesIO
import uuid

import boto3
from botocore.exceptions import ClientError, BotoCoreError
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3DataReader:
    """
    Lector de datos desde S3 para integración de fuentes batch en streaming.
    Soporta Parquet, CSV, JSON con particionamiento temporal.
    """
    
    def __init__(
        self,
        bucket: str,
        prefix: str = '',
        file_format: str = 'parquet',
        s3_client=None,
        partition_keys: Optional[List[str]] = None,
        max_file_size_mb: int = 100
    ):
        self.bucket = bucket
        self.prefix = prefix.rstrip('/')
        self.file_format = file_format.lower()
        self.s3_client = s3_client or boto3.client('s3')
        self.partition_keys = partition_keys or ['year', 'month', 'day', 'hour']
        self.max_file_size_mb = max_file_size_mb
        
        self._validate_bucket_access()
    
    def _validate_bucket_access(self) -> None:
        """Valida acceso al bucket de S3."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
            logger.info(f"Acceso validado al bucket: {self.bucket}")
        
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == '404':
                raise FileNotFoundError(f"Bucket '{self.bucket}' no existe")
            elif error_code == '403':
                raise PermissionError(f"Sin acceso al bucket '{self.bucket}'")
            else:
                raise ConnectionError(f"Error accediendo al bucket: {str(e)}")
    
    def _list_objects(self, max_keys: int = 100) -> List[Dict[str, Any]]:
        """Lista objetos en el prefijo especificado."""
        objects = []
        continuation_token = None
        
        while True:
            params = {
                'Bucket': self.bucket,
                'Prefix': self.prefix,
                'MaxKeys': max_keys
            }
            
            if continuation_token:
                params['ContinuationToken'] = continuation_token
            
            try:
                response = self.s3_client.list_objects_v2(**params)
                
                if 'Contents' in response:
                    for obj in response['Contents']:
                        if self._is_valid_file(obj['Key']):
                            objects.append({
                                'key': obj['Key'],
                                'size': obj['Size'],
                                'last_modified': obj['LastModified'].isoformat()
                            })
                
                if not response.get('IsTruncated', False):
                    break
                
                continuation_token = response.get('NextContinuationToken')
            
            except ClientError as e:
                logger.error(f"Error listando objetos en S3: {str(e)}")
                raise
        
        logger.info(f"Encontrados {len(objects)} objetos en {self.prefix}")
        return objects
    
    def _is_valid_file(self, key: str) -> bool:
        """Verifica si el archivo tiene el formato esperado."""
        valid_extensions = {
            'parquet': ['.parquet', '.pq'],
            'csv': ['.csv'],
            'json': ['.json', '.jsonl']
        }
        
        extensions = valid_extensions.get(self.file_format, [])
        return any(key.lower().endswith(ext) for ext in extensions)
    
    def _read_parquet(self, key: str) -> pd.DataFrame:
        """Lee un archivo Parquet desde S3."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            data = response['Body'].read()
            
            df = pd.read_parquet(BytesIO(data))
            logger.debug(f"Leídos {len(df)} registros de {key}")
            return df
        
        except Exception as e:
            logger.error(f"Error leyendo Parquet {key}: {str(e)}")
            raise
    
    def _read_csv(self, key: str) -> pd.DataFrame:
        """Lee un archivo CSV desde S3."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            data = response['Body'].read().decode('utf-8')
            
            df = pd.read_csv(BytesIO(data.encode('utf-8')))
            logger.debug(f"Leídos {len(df)} registros de {key}")
            return df
        
        except Exception as e:
            logger.error(f"Error leyendo CSV {key}: {str(e)}")
            raise
    
    def _read_json(self, key: str) -> pd.DataFrame:
        """Lee un archivo JSON desde S3."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            data = response['Body'].read().decode('utf-8')
            
            if key.endswith('.jsonl'):
                df = pd.read_json(BytesIO(data.encode('utf-8')), lines=True)
            else:
                df = pd.read_json(BytesIO(data.encode('utf-8')))
            
            logger.debug(f"Leídos {len(df)} registros de {key}")
            return df
        
        except Exception as e:
            logger.error(f"Error leyendo JSON {key}: {str(e)}")
            raise
    
    def _read_file(self, key: str) -> pd.DataFrame:
        """Determina el método de lectura según el formato."""
        readers = {
            'parquet': self._read_parquet,
            'csv': self._read_csv,
            'json': self._read_json
        }
        
        reader = readers.get(self.file_format)
        if not reader:
            raise ValueError(f"Formato no soportado: {self.file_format}")
        
        return reader(key)
    
    def _extract_partition_info(self, key: str) -> Dict[str, Any]:
        """Extrae información de partición de la ruta del archivo."""
        parts = key.split('/')
        partition_info = {}
        
        for part in parts:
            if '=' in part:
                key_part, value_part = part.split('=', 1)
                if key_part in self.partition_keys:
                    partition_info[key_part] = value_part
        
        return partition_info
    
    def read_batch(self, max_files: int = 10) -> List[Dict[str, Any]]:
        """
        Lee un lote de archivos desde S3.
        
        Args:
            max_files: Número máximo de archivos a procesar.
        
        Returns:
            Lista de registros leídos.
        """
        objects = self._list_objects(max_keys=max_files)
        
        if not objects:
            logger.warning(f"No se encontraron archivos en {self.prefix}")
            return []
        
        objects_to_process = objects[:max_files]
        all_records = []
        
        for obj in objects_to_process:
            try:
                df = self._read_file(obj['key'])
                
                partition_info = self._extract_partition_info(obj['key'])
                
                records = df.to_dict('records')
                
                for record in records:
                    record['record_id'] = str(uuid.uuid4())
                    record['ingestion_time'] = datetime.utcnow().isoformat()
                    record['source_key'] = obj['key']
                    record['source_size'] = obj['size']
                    record['source_last_modified'] = obj['last_modified']
                    record.update(partition_info)
                    
                    all_records.append(record)
                
                logger.info(f"Procesados {len(records)} registros de {obj['key']}")
            
            except Exception as e:
                logger.error(f"Error procesando archivo {obj['key']}: {str(e)}")
                continue
        
        logger.info(f"Total de registros leídos: {len(all_records)}")
        return all_records
    
    def read_with_filter(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_files: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Lee archivos filtrados por rango de fechas usando particiones."""
        objects = self._list_objects(max_keys=max_files)
        
        filtered_records = []
        
        for obj in objects:
            partition_info = self._extract_partition_info(obj['key'])
            
            if start_date or end_date:
                record_date = None
                
                if all(k in partition_info for k in ['year', 'month', 'day']):
                    try:
                        record_date = datetime(
                            int(partition_info['year']),
                            int(partition_info['month']),
                            int(partition_info['day'])
                        )
                    except ValueError:
                        continue
                
                if record_date:
                    if start_date and record_date < start_date:
                        continue
                    if end_date and record_date > end_date:
                        continue
            
            try:
                df = self._read_file(obj['key'])
                records = df.to_dict('records')
                
                for record in records:
                    record['record_id'] = str(uuid.uuid4())
                    record['source_key'] = obj['key']
                    record.update(partition_info)
                    filtered_records.append(record)
            
            except Exception as e:
                logger.error(f"Error filtrando {obj['key']}: {str(e)}")
                continue
        
        logger.info(f"Registros obtenidos con filtro: {len(filtered_records)}")
        return filtered_records
    
    def get_partition_info(self) -> List[Dict[str, Any]]:
        """Obtiene información de particiones disponibles."""
        objects = self._list_objects(max_keys=1000)
        
        partitions = {}
        for obj in objects:
            part_info = self._extract_partition_info(obj['key'])
            if part_info:
                key = tuple(sorted(part_info.items()))
                if key not in partitions:
                    partitions[key] = {
                        'partition': part_info,
                        'file_count': 0,
                        'total_size': 0
                    }
                partitions[key]['file_count'] += 1
                partitions[key]['total_size'] += obj['size']
        
        return list(partitions.values())
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.s3_client:
            self.s3_client.close()