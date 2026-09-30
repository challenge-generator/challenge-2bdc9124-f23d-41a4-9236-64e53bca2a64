import logging
from typing import Optional, List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class DataCleaner:
    """Limpieza y normalización de datos estructurados y semiestructurados."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        self.spark = spark
        self.config = config or {}
        self.null_thresholds = self.config.get('null_thresholds', {
            'string': 0.5,
            'numeric': 0.3,
            'timestamp': 0.1
        })
        self.deduplication_key = self.config.get('deduplication_key', 'event_id')
        
    def clean(self, df: DataFrame, source_type: str = 'structured') -> DataFrame:
        """Aplica pipeline completo de limpieza según tipo de fuente."""
        logger.info(f"Iniciando limpieza para fuente tipo: {source_type}")
        
        df = self.remove_duplicates(df)
        df = self.standardize_nulls(df)
        df = self.normalize_strings(df)
        df = self.cast_types(df, source_type)
        df = self.handle_outliers(df)
        
        logger.info(f"Limpieza completada. Registros finales: {df.count()}")
        return df
    
    def remove_duplicates(self, df: DataFrame) -> DataFrame:
        """Elimina duplicados usando clave de deduplicación configurada."""
        if self.deduplication_key in df.columns:
            initial_count = df.count()
            df = df.dropDuplicates([self.deduplication_key])
            removed = initial_count - df.count()
            logger.info(f"Eliminados {removed} registros duplicados")
        else:
            logger.warning(f"Columna de deduplicación '{self.deduplication_key}' no encontrada")
        return df
    
    def standardize_nulls(self, df: DataFrame) -> DataFrame:
        """Estandariza valores nulos y vacíos a formato consistente."""
        string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
        
        for col_name in string_cols:
            df = df.withColumn(
                col_name,
                F.when(
                    (F.col(col_name).isNull()) | (F.trim(F.col(col_name)) == ''),
                    F.lit(None)
                ).otherwise(F.trim(F.col(col_name)))
            )
        
        null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
        logger.info(f"Distribución de nulos por columna: {null_counts.collect()[0].asDict()}")
        return df
    
    def normalize_strings(self, df: DataFrame) -> DataFrame:
        """Normaliza texto: lowercase, remove special chars, estandariza encoding."""
        string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
        
        for col_name in string_cols:
            df = df.withColumn(col_name, F.lower(F.col(col_name)))
            df = df.withColumn(col_name, F.regexp_replace(col_name, r'[^\w\s]', ''))
            df = df.withColumn(col_name, F.regexp_replace(col_name, r'\s+', ' '))
        
        return df
    
    def cast_types(self, df: DataFrame, source_type: str) -> DataFrame:
        """Convierte tipos según la fuente de datos."""
        if source_type == 'json':
            numeric_cols = ['amount', 'quantity', 'price', 'value']
            for col_name in numeric_cols:
                if col_name in df.columns:
                    df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
        
        elif source_type == 'csv':
            timestamp_cols = [c for c in df.columns if 'timestamp' in c.lower() or 'date' in c.lower()]
            for col_name in timestamp_cols:
                df = df.withColumn(
                    col_name,
                    F.to_timestamp(col_name, 'yyyy-MM-dd HH:mm:ss')
                )
        
        return df
    
    def handle_outliers(self, df: DataFrame) -> DataFrame:
        """Detecta y maneja outliers usando método IQR."""
        numeric_cols = [f.name for f in df.schema.fields 
                       if isinstance(f.dataType, (IntegerType, DoubleType))]
        
        for col_name in numeric_cols:
            if col_name in df.columns:
                quantiles = df.approxQuantile(col_name, [0.25, 0.75], 0.05)
                if len(quantiles) == 2 and quantiles[0] is not None:
                    q1, q3 = quantiles
                    iqr = q3 - q1
                    lower_bound = q1 - 3 * iqr
                    upper_bound = q3 + 3 * iqr
                    
                    outlier_count = df.filter(
                        (F.col(col_name) < lower_bound) | (F.col(col_name) > upper_bound)
                    ).count()
                    
                    if outlier_count > 0:
                        logger.warning(f"Columna {col_name}: {outlier_count} outliers detectados")
                        df = df.withColumn(
                            col_name,
                            F.when(
                                (F.col(col_name) < lower_bound) | (F.col(col_name) > upper_bound),
                                F.lit(None)
                            ).otherwise(F.col(col_name))
                        )
        
        return df
    
    def fill_missing_values(self, df: DataFrame, strategy: str = 'forward') -> DataFrame:
        """Rellena valores faltantes según estrategia configurada."""
        if strategy == 'forward':
            window_spec = Window.orderBy('timestamp').rowsBetween(Window.unboundedPre, 0)
            string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
            for col_name in string_cols:
                df = df.withColumn(col_name, F.last(col_name, ignoreNulls=True).over(window_spec))
        elif strategy == 'default':
            df = df.fillna({})
        
        return df


class SemiStructuredDataCleaner(DataCleaner):
    """Especialización para datos semiestructurados (JSON, XML)."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        super().__init__(spark, config)
        self.nested_fields = self.config.get('nested_fields', [])
    
    def flatten_nested_schema(self, df: DataFrame) -> DataFrame:
        """Aplana estructuras anidadas en columnas individuales."""
        for field in self.nested_fields:
            if field in df.columns:
                json_schema = df.select(field).schema[0].dataType
                if isinstance(json_schema, StructType):
                    for subfield in json_schema.fields:
                        df = df.withColumn(
                            f"{field}_{subfield.name}",
                            F.col(f"{field}.{subfield.name}")
                        )
                    df = df.drop(field)
                    logger.info(f"Campo anidado {field} aplanado")
        return df
    
    def extract_array_elements(self, df: DataFrame, array_col: str, prefix: str) -> DataFrame:
        """Extrae elementos de arrays en columnas separadas."""
        if array_col in df.columns:
            max_elements = df.select(F.size(F.col(array_col))).collect()[0][0]
            if max_elements:
                for i in range(min(max_elements, 10)):
                    df = df.withColumn(
                        f"{prefix}_{i}",
                        F.col(array_col)[i]
                    )
                logger.info(f"Extraídos {min(max_elements, 10)} elementos del array {array_col}")
        return df