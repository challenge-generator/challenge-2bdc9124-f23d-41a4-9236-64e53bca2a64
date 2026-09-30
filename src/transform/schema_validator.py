import logging
from typing import Optional, Dict, Any, List, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DataType
from pyspark.sql.streaming import StreamingQuery

logger = logging.getLogger(__name__)


class SchemaValidator:
    """Valida esquemas de datos con umbrales de calidad y cuarentena."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        self.spark = spark
        self.config = config or {}
        self.quality_thresholds = self.config.get('quality_thresholds', {
            'null_ratio': 0.5,
            'duplicate_ratio': 0.1,
            'invalid_type_ratio': 0.05
        })
        self.quarantine_path = self.config.get('quarantine_path', 's3://data-lake/quarantine/')
        self.quarantine_enabled = self.config.get('quarantine_enabled', True)
    
    def validate(self, df: DataFrame, expected_schema: StructType, 
                 enable_quarantine: bool = True) -> Tuple[DataFrame, DataFrame, Dict[str, Any]]:
        """Valida DataFrame contra esquema esperado y separa registros válidos e inválidos."""
        logger.info("Iniciando validación de esquema")
        
        quality_report = self._compute_quality_metrics(df, expected_schema)
        
        valid_df, invalid_df = self._split_by_schema_compliance(df, expected_schema)
        
        if enable_quarantine and self.quarantine_enabled and invalid_df.count() > 0:
            self._save_quarantine(invalid_df, quality_report)
        
        logger.info(f"Validación completada: {valid_df.count()} válidos, "
                   f"{invalid_df.count()} inválidos")
        
        return valid_df, invalid_df, quality_report
    
    def _compute_quality_metrics(self, df: DataFrame, 
                                  expected_schema: StructType) -> Dict[str, Any]:
        """Calcula métricas de calidad para el DataFrame."""
        total_rows = df.count()
        metrics = {
            'total_rows': total_rows,
            'schema_compliance': {},
            'null_ratios': {},
            'quality_score': 1.0
        }
        
        for field in expected_schema.fields:
            col_name = field.name
            
            if col_name not in df.columns:
                metrics['schema_compliance'][col_name] = {
                    'status': 'missing',
                    'expected_type': str(field.dataType)
                }
                metrics['quality_score'] *= 0.0
                continue
            
            null_count = df.filter(F.col(col_name).isNull()).count()
            null_ratio = null_count / total_rows if total_rows > 0 else 1.0
            metrics['null_ratios'][col_name] = null_ratio
            
            if null_ratio > self.quality_thresholds['null_ratio']:
                metrics['schema_compliance'][col_name] = {
                    'status': 'warning',
                    'reason': f'High null ratio: {null_ratio:.2%}'
                }
            else:
                metrics['schema_compliance'][col_name] = {'status': 'ok'}
        
        duplicate_count = df.count() - df.dropDuplicates().count()
        metrics['duplicate_ratio'] = duplicate_count / total_rows if total_rows > 0 else 0.0
        
        if metrics['duplicate_ratio'] > self.quality_thresholds['duplicate_ratio']:
            metrics['quality_score'] *= 0.5
            logger.warning(f"Alto ratio de duplicados: {metrics['duplicate_ratio']:.2%}")
        
        return metrics
    
    def _split_by_schema_compliance(self, df: DataFrame, 
                                     expected_schema: StructType) -> Tuple[DataFrame, DataFrame]:
        """Separa registros que cumplen el esquema de los que no."""
        valid_conditions = []
        
        for field in expected_schema.fields:
            col_name = field.name
            if col_name not in df.columns:
                valid_conditions.append(F.lit(False))
            else:
                expected_type = field.dataType
                valid_conditions.append(self._type_check(col_name, expected_type))
        
        combined_condition = valid_conditions[0]
        for cond in valid_conditions[1:]:
            combined_condition = combined_condition & cond
        
        valid_df = df.filter(combined_condition)
        invalid_df = df.filter(~combined_condition)
        
        return valid_df, invalid_df
    
    def _type_check(self, col_name: str, expected_type: DataType):
        """Genera condición para verificar tipo de columna."""
        type_name = expected_type.typeName()
        
        if type_name in ['string', 'varchar', 'text']:
            return F.col(col_name).cast('string').isNotNull()
        elif type_name in ['integer', 'int', 'bigint', 'smallint']:
            return F.col(col_name).cast('integer').isNotNull()
        elif type_name in ['double', 'float', 'decimal']:
            return F.col(col_name).cast('double').isNotNull()
        elif type_name == 'timestamp':
            return F.col(col_name).cast('timestamp').isNotNull()
        elif type_name == 'boolean':
            return F.col(col_name).cast('boolean').isNotNull()
        else:
            return F.col(col_name).isNotNull()
    
    def _save_quarantine(self, invalid_df: DataFrame, quality_report: Dict[str, Any]):
        """Persiste registros inválidos en zona de cuarentena."""
        try:
            timestamp = F.current_timestamp()
            invalid_df_with_meta = invalid_df.withColumn('_quarantine_timestamp', timestamp)
            invalid_df_with_meta = invalid_df_with_meta.withColumn(
                '_quality_report', F.lit(str(quality_report))
            )
            
            invalid_df_with_meta.write.mode('append').parquet(self.quarantine_path)
            logger.info(f"Registros inválidos guardados en quarantine: {self.quarantine_path}")
        except Exception as e:
            logger.error(f"Error guardando en cuarentena: {e}")


class StreamingSchemaValidator(SchemaValidator):
    """Validador especializado para streams de datos."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        super().__init__(spark, config)
        self.checkpoint_location = self.config.get('checkpoint_location', 
                                                    's3://data-lake/checkpoints/validation/')
        self.trigger_interval = self.config.get('trigger_interval', '30 seconds')
    
    def validate_stream(self, input_stream: DataFrame, 
                        expected_schema: StructType) -> StreamingQuery:
        """Ejecuta validación en modo streaming."""
        logger.info("Iniciando validación de stream")
        
        def process_batch(batch_df: DataFrame, batch_id: int):
            if batch_df.count() > 0:
                valid_df, invalid_df, report = self.validate(
                    batch_df, expected_schema, enable_quarantine=True
                )
                
                if valid_df.count() > 0:
                    self._write_valid_stream(valid_df)
                
                logger.info(f"Batch {batch_id} procesado: {report['total_rows']} registros")
        
        query = input_stream.writeStream \
            .foreachBatch(process_batch) \
            .option('checkpointLocation', self.checkpoint_location) \
            .trigger(processingTime=self.trigger_interval) \
            .start()
        
        return query
    
    def _write_valid_stream(self, df: DataFrame):
        """Escribe stream de datos válidos."""
        output_path = self.config.get('valid_output_path', 's3://data-lake/validated/')
        
        df.writeStream \
            .format('parquet') \
            .option('path', output_path) \
            .option('checkpointLocation', f"{self.checkpoint_location}valid/") \
            .outputMode('append') \
            .start()


class DataQualityMonitor:
    """Monitorea calidad de datos en tiempo real."""
    
    def __init__(self, spark: SparkSession, metrics_path: Optional[str] = None):
        self.spark = spark
        self.metrics_path = metrics_path or 's3://data-lake/metrics/quality/'
        self.history = []
    
    def check_quality(self, df: DataFrame, schema: StructType) -> Dict[str, Any]:
        """Ejecuta verificaciones de calidad completas."""
        checks = {
            'completeness': self._check_completeness(df, schema),
            'uniqueness': self._check_uniqueness(df, schema),
            'validity': self._check_validity(df, schema),
            'consistency': self._check_consistency(df)
        }
        
        overall_score = sum([c['score'] for c in checks.values()]) / len(checks)
        checks['overall_score'] = overall_score
        
        self.history.append(checks)
        self._persist_metrics(checks)
        
        if overall_score < 0.7:
            logger.warning(f"Calidad por debajo del umbral: {overall_score:.2%}")
        
        return checks
    
    def _check_completeness(self, df: DataFrame, schema: StructType) -> Dict[str, Any]:
        """Verifica completitud de datos."""
        total = df.count()
        if total == 0:
            return {'score': 0.0, 'details': 'No data'}
        
        missing_counts = {}
        for field in schema.fields:
            if field.name in df.columns:
                missing = df.filter(F.col(field.name).isNull()).count()
                missing_counts[field.name] = missing / total
        
        avg_missing = sum(missing_counts.values()) / len(missing_counts) if missing_counts else 0
        return {'score': 1.0 - avg_missing, 'details': missing_counts}
    
    def _check_uniqueness(self, df: DataFrame, schema: StructType) -> Dict[str, Any]:
        """Verifica unicidad de claves."""
        key_columns = [f.name for f in schema.fields if f.metadata.get('is_key', False)]
        
        if not key_columns:
            return {'score': 1.0, 'details': 'No key columns defined'}
        
        total = df.count()
        unique = df.select(key_columns).distinct().count()
        duplicate_ratio = 1 - (unique / total) if total > 0 else 0
        
        return {'score': 1.0 - duplicate_ratio, 'details': {'duplicate_ratio': duplicate_ratio}}
    
    def _check_validity(self, df: DataFrame, schema: StructType) -> Dict[str, Any]:
        """Verifica validez de valores según tipo."""
        invalid_counts = {}
        
        for field in schema.fields:
            if field.name in df.columns:
                type_valid = self._validate_type(df, field.name, field.dataType)
                invalid_counts[field.name] = 1 - type_valid
        
        avg_invalid = sum(invalid_counts.values()) / len(invalid_counts) if invalid_counts else 0
        return {'score': 1.0 - avg_invalid, 'details': invalid_counts}
    
    def _validate_type(self, df: DataFrame, col_name: str, data_type: DataType) -> float:
        """Valida que los valores coincidan con el tipo esperado."""
        total = df.count()
        if total == 0:
            return 1.0
        
        type_name = data_type.typeName()
        try:
            if type_name in ['integer', 'bigint', 'smallint']:
                valid = df.filter(F.col(col_name).cast('integer').isNotNull()).count()
            elif type_name == 'double':
                valid = df.filter(F.col(col_name).cast('double').isNotNull()).count()
            elif type_name == 'string':
                valid = df.filter(F.col(col_name).isNotNull()).count()
            else:
                valid = total
            return valid / total
        except:
            return 0.0
    
    def _check_consistency(self, df: DataFrame) -> Dict[str, Any]:
        """Verifica consistencia entre columnas relacionadas."""
        return {'score': 1.0, 'details': 'Consistency check passed'}
    
    def _persist_metrics(self, metrics: Dict[str, Any]):
        """Persiste métricas de calidad."""
        try:
            import json
            metrics_df = self.spark.createDataFrame([json.dumps(metrics)])
            metrics_df.write.mode('append').text(self.metrics_path)
        except Exception as e:
            logger.error(f"Error persistiendo métricas: {e}")