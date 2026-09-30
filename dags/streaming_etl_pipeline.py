"""
DAG de Airflow para orquestar el pipeline ETL en streaming.
Coordina las etapas de extracción, transformación y carga con gestión de dependencias.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

def get_config():
    """Carga configuración del pipeline desde Variables de Airflow."""
    config = {
        'kafka_bootstrap_servers': Variable.get('kafka_bootstrap_servers', default_var='localhost:9092'),
        'kafka_topic': Variable.get('kafka_topic', default_var='raw_events'),
        's3_bucket': Variable.get('s3_bucket', default_var='data-lake-raw'),
        's3_input_prefix': Variable.get('s3_input_prefix', default_var='batch/input/'),
        'redshift_cluster': Variable.get('redshift_cluster', default_var='analytics-cluster'),
        'redshift_database': Variable.get('redshift_database', default_var='analytics'),
        'redshift_schema': Variable.get('redshift_schema', default_var='staging'),
        'processing_batch_size': int(Variable.get('processing_batch_size', default_var='1000')),
        'checkpoint_enabled': Variable.get('checkpoint_enabled', default_var='true'),
    }
    logger.info(f"Configuración cargada: {json.dumps({k: v for k, v in config.items()}, default=str)}")
    return config


def extract_from_kafka(**context):
    """Ejecuta la etapa de extracción desde Kafka."""
    from src.extract.kafka_consumer import KafkaDataConsumer
    
    config = get_config()
    ti = context['ti']
    
    consumer = KafkaDataConsumer(
        bootstrap_servers=config['kafka_bootstrap_servers'],
        topic=config['kafka_topic'],
        group_id='etl_streaming_group',
        batch_size=config['processing_batch_size'],
        checkpoint_enabled=config['checkpoint_enabled'] == 'true'
    )
    
    try:
        records = consumer.consume_batch(timeout_seconds=60)
        logger.info(f"Extraídos {len(records)} registros de Kafka")
        
        if not records:
            logger.warning("No se recibieron registros en esta ejecución")
            return {'status': 'no_data', 'count': 0}
        
        extracted_data = []
        for record in records:
            extracted_data.append({
                'message_id': record.get('message_id'),
                'timestamp': record.get('timestamp'),
                'payload': record.get('payload'),
                'source': 'kafka'
            })
        
        ti.xcom_push(key='extracted_data', value=extracted_data)
        ti.xcom_push(key='extraction_count', value=len(extracted_data))
        
        return {'status': 'success', 'count': len(extracted_data)}
    
    except Exception as e:
        logger.error(f"Error en extracción Kafka: {str(e)}")
        raise AirflowException(f"Extracción fallida: {str(e)}")
    finally:
        consumer.close()


def extract_from_s3(**context):
    """Ejecuta la etapa de extracción desde S3 para datos batch."""
    from src.extract.s3_reader import S3DataReader
    
    config = get_config()
    ti = context['ti']
    
    reader = S3DataReader(
        bucket=config['s3_bucket'],
        prefix=config['s3_input_prefix'],
        file_format='parquet'
    )
    
    try:
        s3_records = reader.read_batch(max_files=10)
        logger.info(f"Extraídos {len(s3_records)} registros de S3")
        
        if not s3_records:
            logger.warning("No se encontraron archivos en S3 para procesar")
            return {'status': 'no_data', 'count': 0}
        
        extracted_data = []
        for record in s3_records:
            extracted_data.append({
                'message_id': record.get('record_id'),
                'timestamp': record.get('ingestion_time'),
                'payload': record.get('data'),
                'source': 's3'
            })
        
        existing_data = ti.xcom_pull(key='extracted_data', task_ids='extract_kafka')
        if existing_data:
            combined = existing_data + extracted_data
            ti.xcom_push(key='extracted_data', value=combined)
            ti.xcom_push(key='extraction_count', value=len(combined))
        else:
            ti.xcom_push(key='extracted_data', value=extracted_data)
            ti.xcom_push(key='extraction_count', value=len(extracted_data))
        
        return {'status': 'success', 'count': len(extracted_data)}
    
    except Exception as e:
        logger.error(f"Error en extracción S3: {str(e)}")
        raise AirflowException(f"Extracción S3 fallida: {str(e)}")


def transform_data(**context):
    """Ejecuta la etapa de transformación de datos."""
    from src.transform.data_cleaner import DataCleaner
    from src.transform.schema_validator import SchemaValidator
    from src.transform.feature_engineering import FeatureEngineer
    
    ti = context['ti']
    config = get_config()
    
    extracted_data = ti.xcom_pull(key='extracted_data', task_ids='extract_kafka')
    if not extracted_data:
        extracted_data = ti.xcom_pull(key='extracted_data', task_ids='extract_s3')
    
    if not extracted_data:
        raise AirflowException("No hay datos para transformar")
    
    logger.info(f"Transformando {len(extracted_data)} registros")
    
    cleaner = DataCleaner()
    validator = SchemaValidator(schema_path='data/schemas/input_schema.json')
    engineer = FeatureEngineer()
    
    valid_records = []
    invalid_records = []
    
    for record in extracted_data:
        try:
            cleaned = cleaner.clean(record)
            
            is_valid, errors = validator.validate(cleaned)
            
            if is_valid:
                enriched = engineer.engineer_features(cleaned)
                valid_records.append(enriched)
            else:
                record['validation_errors'] = errors
                invalid_records.append(record)
                logger.warning(f"Registro inválido: {errors}")
        
        except Exception as e:
            logger.error(f"Error transformando registro {record.get('message_id')}: {str(e)}")
            record['transform_error'] = str(e)
            invalid_records.append(record)
    
    logger.info(f"Transformación completa: {len(valid_records)} válidos, {len(invalid_records)} inválidos")
    
    ti.xcom_push(key='transformed_data', value=valid_records)
    ti.xcom_push(key='quarantined_data', value=invalid_records)
    ti.xcom_push(key='transformation_stats', value={
        'valid': len(valid_records),
        'invalid': len(invalid_records)
    })
    
    return {'status': 'success', 'valid': len(valid_records), 'invalid': len(invalid_records)}


def load_to_s3(**context):
    """Carga datos transformados a S3 en formato Parquet."""
    from src.load.s3_writer import S3DataWriter
    
    config = get_config()
    ti = context['ti']
    
    transformed_data = ti.xcom_pull(key='transformed_data')
    
    if not transformed_data:
        logger.warning("No hay datos transformados para cargar a S3")
        return {'status': 'skipped', 'count': 0}
    
    writer = S3DataWriter(
        bucket=config['s3_bucket'],
        prefix='processed/',
        partition_by=['timestamp_date'],
        file_format='parquet',
        compression='snappy'
    )
    
    try:
        written = writer.write_batch(transformed_data)
        logger.info(f"Cargados {written} registros a S3")
        
        ti.xcom_push(key='s3_load_count', value=written)
        return {'status': 'success', 'count': written}
    
    except Exception as e:
        logger.error(f"Error cargando a S3: {str(e)}")
        raise AirflowException(f"Carga S3 fallida: {str(e)}")


def load_to_redshift(**context):
    """Carga datos transformados a Redshift."""
    from src.load.redshift_writer import RedshiftDataWriter
    
    config = get_config()
    ti = context['ti']
    
    transformed_data = ti.xcom_pull(key='transformed_data')
    
    if not transformed_data:
        logger.warning("No hay datos transformados para cargar a Redshift")
        return {'status': 'skipped', 'count': 0}
    
    writer = RedshiftDataWriter(
        cluster_identifier=config['redshift_cluster'],
        database=config['redshift_database'],
        schema=config['redshift_schema'],
        table='events'
    )
    
    try:
        written = writer.write_batch(transformed_data)
        logger.info(f"Cargados {written} registros a Redshift")
        
        ti.xcom_push(key='redshift_load_count', value=written)
        return {'status': 'success', 'count': written}
    
    except Exception as e:
        logger.error(f"Error cargando a Redshift: {str(e)}")
        raise AirflowException(f"Carga Redshift fallida: {str(e)}")


def check_data_quality(**context):
    """Verifica la calidad de los datos después de la transformación."""
    ti = context['ti']
    
    stats = ti.xcom_pull(key='transformation_stats')
    
    if not stats:
        logger.warning("No hay estadísticas de transformación disponibles")
        return {'status': 'unknown'}
    
    total = stats.get('valid', 0) + stats.get('invalid', 0)
    if total == 0:
        logger.warning("No hay datos para validar calidad")
        return {'status': 'no_data'}
    
    quality_ratio = stats.get('valid', 0) / total
    
    logger.info(f"Calidad de datos: {quality_ratio:.2%} ({stats.get('valid')}/{total})")
    
    if quality_ratio < 0.95:
        logger.warning(f"Calidad de datos por debajo del umbral: {quality_ratio:.2%}")
        return {'status': 'low_quality', 'ratio': quality_ratio}
    
    return {'status': 'pass', 'ratio': quality_ratio}


def generate_lineage_report(**context):
    """Genera reporte de linaje de datos."""
    ti = context['ti']
    
    extraction_count = ti.xcom_pull(key='extraction_count')
    transformation_stats = ti.xcom_pull(key='transformation_stats')
    s3_count = ti.xcom_pull(key='s3_load_count')
    redshift_count = ti.xcom_pull(key='redshift_load_count')
    
    report = {
        'dag_run_id': context['run_id'],
        'execution_date': context['ds'],
        'extracted_count': extraction_count,
        'transformed_stats': transformation_stats,
        's3_loaded_count': s3_count,
        'redshift_loaded_count': redshift_count,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    logger.info(f"Reporte de linaje: {json.dumps(report, default=str)}")
    
    return report


with DAG(
    dag_id='streaming_etl_pipeline',
    default_args=DEFAULT_ARGS,
    description='Pipeline ETL en streaming para procesamiento de datos en tiempo real',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'streaming', 'data-engineering'],
) as dag:
    
    start = DummyOperator(task_id='start')
    
    with TaskGroup('extract_group') as extract_group:
        extract_kafka = PythonOperator(
            task_id='extract_kafka',
            python_callable=extract_from_kafka,
            provide_context=True,
        )
        
        extract_s3 = PythonOperator(
            task_id='extract_s3',
            python_callable=extract_from_s3,
            provide_context=True,
        )
    
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        provide_context=True,
    )
    
    check_quality = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality,
        provide_context=True,
    )
    
    with TaskGroup('load_group') as load_group:
        load_s3 = PythonOperator(
            task_id='load_s3',
            python_callable=load_to_s3,
            provide_context=True,
        )
        
        load_redshift = PythonOperator(
            task_id='load_redshift',
            python_callable=load_to_redshift,
            provide_context=True,
        )
    
    lineage = PythonOperator(
        task_id='generate_lineage_report',
        python_callable=generate_lineage_report,
        provide_context=True,
    )
    
    end = DummyOperator(task_id='end', trigger_rule='none_failed_or_skipped')
    
    start >> extract_group >> transform >> check_quality >> load_group >> lineage >> end