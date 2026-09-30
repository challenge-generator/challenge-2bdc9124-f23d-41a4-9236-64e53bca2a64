import logging
from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType

logger = logging.getLogger(__name__)


class FeatureEngineer:
    """Genera características derivadas y aplica reglas de negocio."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        self.spark = spark
        self.config = config or {}
        self.aggregation_window = self.config.get('aggregation_window', '1 hour')
        self.feature_prefix = self.config.get('feature_prefix', 'feat_')
    
    def engineer(self, df: DataFrame, rules: List[Dict[str, Any]]) -> DataFrame:
        """Aplica conjunto de reglas de ingeniería de características."""
        logger.info(f"Aplicando {len(rules)} reglas de ingeniería de características")
        
        for rule in rules:
            rule_type = rule.get('type')
            
            if rule_type == 'aggregation':
                df = self.add_aggregations(df, rule.get('columns', []), rule.get('group_by'))
            elif rule_type == 'window':
                df = self.add_window_features(df, rule.get('columns', []), rule.get('window_size'))
            elif rule_type == 'derived':
                df = self.add_derived_column(df, rule.get('name'), rule.get('expression'))
            elif rule_type == 'category_encoding':
                df = self.encode_category(df, rule.get('column'), rule.get('method', 'label'))
            elif rule_type == 'time_features':
                df = self.add_time_features(df, rule.get('timestamp_column'))
            
            logger.info(f"Regla '{rule_type}' aplicada")
        
        return df
    
    def add_aggregations(self, df: DataFrame, agg_cols: List[str], group_by: Optional[str]) -> DataFrame:
        """Calcula agregaciones por grupo."""
        if not group_by or group_by not in df.columns:
            logger.warning(f"Columna de grupo '{group_by}' no encontrada")
            return df
        
        agg_exprs = []
        for col in agg_cols:
            if col in df.columns:
                agg_exprs.extend([
                    F.avg(col).alias(f"{self.feature_prefix}{col}_avg"),
                    F.sum(col).alias(f"{self.feature_prefix}{col}_sum"),
                    F.max(col).alias(f"{self.feature_prefix}{col}_max"),
                    F.min(col).alias(f"{self.feature_prefix}{col}_min"),
                    F.stddev(col).alias(f"{self.feature_prefix}{col}_stddev")
                ])
        
        if agg_exprs:
            df = df.groupBy(group_by).agg(*agg_exprs)
            logger.info(f"Agregaciones calculadas para {len(agg_cols)} columnas")
        
        return df
    
    def add_window_features(self, df: DataFrame, window_cols: List[str], window_size: str) -> DataFrame:
        """Añade características de ventana temporal."""
        if 'timestamp' not in df.columns:
            logger.warning("Columna 'timestamp' no encontrada para window features")
            return df
        
        window_spec = Window.orderBy('timestamp').rowsBetween(-self._parse_window(window_size), 0)
        
        for col in window_cols:
            if col in df.columns:
                df = df.withColumn(
                    f"{self.feature_prefix}{col}_rolling_avg",
                    F.avg(col).over(window_spec)
                )
                df = df.withColumn(
                    f"{self.feature_prefix}{col}_rolling_sum",
                    F.sum(col).over(window_spec)
                )
        
        return df
    
    def add_derived_column(self, df: DataFrame, name: str, expression: str) -> DataFrame:
        """Crea columna derivada desde expresión SQL."""
        try:
            df = df.withColumn(name, F.expr(expression))
            logger.info(f"Columna derivada '{name}' creada")
        except Exception as e:
            logger.error(f"Error creando columna derivada '{name}': {e}")
        return df
    
    def encode_category(self, df: DataFrame, col: str, method: str = 'label') -> DataFrame:
        """Codifica variables categóricas."""
        if col not in df.columns:
            return df
        
        if method == 'label':
            unique_values = df.select(col).distinct().collect()
            mapping = {row[col]: idx for idx, row in enumerate(unique_values)}
            broadcast_mapping = self.spark.sparkContext.broadcast(mapping)
            
            df = df.withColumn(
                f"{self.feature_prefix}{col}_encoded",
                F.udf(lambda x: broadcast_mapping.value.get(x, -1), IntegerType())(F.col(col))
            )
        elif method == 'onehot':
            df = df.withColumn(f"{self.feature_prefix}{col}_binary", F.lit(1))
            
        logger.info(f"Categoría '{col}' codificada con método '{method}'")
        return df
    
    def add_time_features(self, df: DataFrame, timestamp_col: str) -> DataFrame:
        """Extrae características temporales de columna de timestamp."""
        if timestamp_col not in df.columns:
            logger.warning(f"Columna de timestamp '{timestamp_col}' no encontrada")
            return df
        
        df = df.withColumn(f"{self.feature_prefix}hour", F.hour(F.col(timestamp_col)))
        df = df.withColumn(f"{self.feature_prefix}day_of_week", F.dayofweek(F.col(timestamp_col)))
        df = df.withColumn(f"{self.feature_prefix}day_of_month", F.dayofmonth(F.col(timestamp_col)))
        df = df.withColumn(f"{self.feature_prefix}month", F.month(F.col(timestamp_col)))
        df = df.withColumn(f"{self.feature_prefix}year", F.year(F.col(timestamp_col)))
        df = df.withColumn(f"{self.feature_prefix}is_weekend", 
                          F.when(F.dayofweek(F.col(timestamp_col)).isin([1, 7]), 1).otherwise(0))
        
        logger.info(f"Características temporales extraídas de '{timestamp_col}'")
        return df
    
    def _parse_window(self, window_size: str) -> int:
        """Convierte tamaño de ventana a número de filas."""
        unit = window_size.split()[1] if ' ' in window_size else 'hours'
        value = int(window_size.split()[0]) if ' ' in window_size else 1
        
        multipliers = {'minutes': 60, 'hours': 60, 'days': 1440}
        return value * multipliers.get(unit, 60)


class BusinessRulesEngine:
    """Motor de reglas de negocio para transformación de datos."""
    
    def __init__(self, spark: SparkSession, rules_config: Optional[Dict[str, Any]] = None):
        self.spark = spark
        self.rules_config = rules_config or {}
        self.rules = self._load_rules()
    
    def _load_rules(self) -> List[Dict[str, Any]]:
        """Carga reglas desde configuración."""
        return self.rules_config.get('business_rules', [
            {'name': 'valid_amount', 'condition': 'amount > 0', 'action': 'pass'},
            {'name': 'valid_category', 'condition': 'category IS NOT NULL', 'action': 'quarantine'},
            {'name': 'high_value_transaction', 'condition': 'amount > 10000', 'action': 'flag'}
        ])
    
    def apply_rules(self, df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
        """Aplica reglas y separa en flujos: válidos, cuarentena y flagged."""
        valid_df = df
        quarantine_df = self.spark.createDataFrame([], df.schema)
        flagged_df = self.spark.createDataFrame([], df.schema)
        
        for rule in self.rules:
            condition = rule.get('condition', '')
            action = rule.get('action', 'pass')
            
            if not condition:
                continue
            
            try:
                valid_mask = F.expr(condition)
                
                if action == 'quarantine':
                    new_quarantine = valid_df.filter(~valid_mask)
                    quarantine_df = quarantine_df.union(new_quarantine)
                    valid_df = valid_df.filter(valid_mask)
                    
                elif action == 'flag':
                    new_flagged = valid_df.filter(valid_mask)
                    flagged_df = flagged_df.union(new_flagged)
                    
            except Exception as e:
                logger.error(f"Error aplicando regla '{rule.get('name')}': {e}")
        
        logger.info(f"Reglas aplicadas: {valid_df.count()} válidos, "
                   f"{quarantine_df.count()} cuarentena, {flagged_df.count()} flaggeados")
        
        return valid_df, quarantine_df, flagged_df