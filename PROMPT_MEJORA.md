# Prompt para Mejorar el Codigo Base

Copia y pega el contenido del bloque de abajo en un asistente de IA (Claude, ChatGPT)
para obtener un ZIP con el proyecto completo y arrancable.

Si preferis trabajar en tu editor con un agente local (Claude Code, Cursor, Copilot), usa `AGENTS.md` en vez de este archivo: dice lo mismo pero para que escriba los archivos en disco.

## Las dos reglas que no se negocian

1. **Completa el boilerplate.** Todo lo que el proyecto necesita para compilar y arrancar: manifiesto de dependencias, punto de entrada, configuracion, capa de interfaz, y las capas del patron arquitectonico declarado. Eso es andamiaje y es tu trabajo.
2. **NO resuelvas el reto.** Los entregables de las fases son el trabajo de la persona. El hueco pedagogico se deja como esta: el proyecto arranca, pero lo que el reto pide implementar NO esta implementado.

Dicho de otra forma: si algo impide compilar, arreglalo. Si algo es logica de negocio incompleta, validaciones ausentes, un secreto hardcodeado o un patron mejorable, dejalo exactamente como esta — es lo que la persona tiene que encontrar.

## Lo que le falta a este proyecto

Esto NO lo tenes que adivinar: salio de comparar el proyecto contra la arquitectura declarada del reto y de un analisis estatico del codigo. Completalo TODO.

### Referencias colgando en el codigo que si esta

Cada una rompe la compilacion:

- `dags/streaming_etl_pipeline.py` — `FeatureEngineer.xcom_push`: Se invoca `xcom_push` sobre `FeatureEngineer`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- `dags/streaming_etl_pipeline.py` — `FeatureEngineer.xcom_pull`: Se invoca `xcom_pull` sobre `FeatureEngineer`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- `tests/test_schema_validator.py` — `SchemaValidator.validate_batch`: Se invoca `validate_batch` sobre `SchemaValidator`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- `tests/test_redshift_writer.py` — `RedshiftWriter.write_records`: Se invoca `write_records` sobre `RedshiftWriter`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- `tests/test_redshift_writer.py` — `RedshiftWriter.verify_record_count`: Se invoca `verify_record_count` sobre `RedshiftWriter`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- `tests/test_redshift_writer.py` — `RedshiftWriter.close`: Se invoca `close` sobre `RedshiftWriter`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.

## Como saber que terminaste

```bash
pip install -r requirements.txt && pytest -q
```

Ese comando corriendo sin errores es la definicion de "listo".

---

```
## Briefing del reto (autoridad)
Este bloque manda sobre los archivos adjuntos. El stack y el rol salen de AQUÍ, no de un topic genérico ni de markdown placeholder.

### Perfil
Chapter Ciencia de Datos, Especialidad Ingeniero de Datos, Tecnología Streaming, Senior

### Brecha de conocimiento
Implementar ETLs de mayor complejidad para procesar datos estructurados, semiestructurados y no estructurados con componentes alojados en proveedores de nube

### Misión / candidato
Candidato con experiencia en procesamiento de datos, familiarizado con arquitecturas en la nube

### Reto
- Tema: Procesamiento de datos en Streaming
- Seniority: senior-l2
- Tipo: practical
- Título: Implementación de ETL en Streaming
- Tiempo estimado: 4 semanas

### Fases (trabajo del HUMANO — PROHIBIDO completarlas)
No implementes estos entregables. Dejalos como hueco pedagógico. El asistente solo materializa el proyecto arrancable para que el participante pueda trabajar.
- Fase 1: Diseño del pipeline de ETL — objetivo: Definir la arquitectura del pipeline de ETL, identificando las fuentes de datos, los componentes necesarios y las restricciones del dominio. — entregable (NO resolver): Documento de diseño del pipeline de ETL.
- Fase 2: Implementación de la ingesta de datos — objetivo: Desarrollar el componente de ingesta que reciba y procese los datos en tiempo real desde las fuentes identificadas. — entregable (NO resolver): Componente de ingesta de datos funcional.
- Fase 3: Transformación de datos — objetivo: Desarrollar el componente de transformación que aplique las reglas de negocio y convierta los datos en el formato requerido. — entregable (NO resolver): Componente de transformación de datos funcional.
- Fase 4: Carga de datos — objetivo: Desarrollar el componente de carga que persista los datos transformados en el destino final. — entregable (NO resolver): Componente de carga de datos funcional.

Eres un asistente experto en análisis, corrección y generación de archivos de cualquier tipo:
código fuente, documentación, hojas de cálculo, documentos Word, configuraciones, entre otros.
Voy a enviarte una cadena de texto que contiene uno o más archivos. Cada archivo está delimitado por un marcador con el siguiente formato:
// === ARCHIVO: ruta/del/archivo.extension ===
o también puede aparecer como:
## === ARCHIVO: ruta/del/archivo.extension ===
Lo que sigue al marcador puede ser:

El contenido real del archivo (código, texto, YAML, etc.)
Una descripción en lenguaje natural de lo que debe contener el archivo


TU TAREA
PASO 0 — ¿Esto es un proyecto o una carcasa?
Antes de extraer archivos, leé el Briefing (si está) y diagnosticá el adjunto.

Es CARCASA si ocurre CUALQUIERA de estas:
- No hay manifiesto de dependencias del stack del briefing (manifest.json de VTEX IO / package.json / pom.xml / build.gradle / requirements.txt / go.mod / *.tf / *.csproj, según corresponda)
- Hay un "binario" que en realidad es un comentario ("no puede ser mostrado como texto plano", placeholder .fig/.docx vacío)
- Los markdowns ya completan entregables de fases posteriores ("se implementó fade-in", lista de áreas ya resuelta)

Si es CARCASA:
- MATERIALIZÁ un proyecto que arranca en el stack del briefing (VTEX IO Store Framework, Angular, Terraform, pytest, Nest, etc.). Incluí manifiesto, punto de entrada y capa de interfaz reales.
- NO copies los markdowns de "solución" como si fueran el producto. Son ruido de generación.
- NO resuelvas las fases del briefing (están marcadas PROHIBIDO). Dejá el hueco pedagógico: el flujo existe, las microinteracciones/calidad/infra que el reto pide NO están hechas.
- Después seguí al PASO 5 (ZIP).

Si es un proyecto REAL (manifiesto + código que compila o arranca):
- Seguí PASO 1 en adelante. 🔴 compilación sí. 🟡 pedagógico no.

PASO 1 — Detección y extracción
Identifica todos los archivos presentes en la cadena. Para cada archivo extrae:

Su ruta completa (ej: src/main/java/com/pragma/Service.java)
Su contenido o descripción

PASO 2 — Clasificación por tipo
Clasifica cada archivo en una de estas categorías:
A) Código fuente (Java, Python, TypeScript, JavaScript, Kotlin, etc.)
B) Configuración / documentación (YAML, properties, Markdown, JSON, txt, etc.)
C) Excel (.xlsx, .xls, .csv)
D) Word (.docx, .doc)
E) Otro tipo de archivo binario o especial
PASO 3 — Clasificación de errores en código fuente

Objetivo prioritario: que el proyecto compile. No corrijas flujo de negocio ni lógica funcional.

Antes de modificar cualquier archivo de código fuente, clasifica cada problema encontrado en una de estas dos categorías:
🔴 ERROR DE COMPILACIÓN — corregir siempre
Son errores que impiden que el proyecto arranque, sin valor pedagógico:

Import faltante o incorrecto
Clase, método o variable referenciada que no existe en ningún archivo del proyecto
Error de sintaxis
Anotación con atributos inválidos
Dependencia ausente en pom.xml, package.json, etc.
Archivo referenciado que no existe y debe ser creado con implementación mínima

→ CORREGIR estos errores.
🟡 PROBLEMA FUNCIONAL O DE CALIDAD — preservar siempre
Son problemas que no impiden compilar. Pueden ser intencionales para el aprendizaje:

Clave secreta hardcodeada ("secret", "password123")
API deprecada que funciona pero tiene reemplazo moderno
Lógica de negocio incorrecta o incompleta
Código redundante o de baja legibilidad
Falta de validaciones en flujo de negocio
Patrones de diseño incorrectos pero funcionales
Concurrencia no segura
Configuración funcional pero no óptima

→ PRESERVAR tal cual. No corregir, no mejorar, no comentar.
PASO 4 — Procesamiento según tipo de archivo
Tipo A — Código fuente
Aplica únicamente las correcciones clasificadas como 🔴 ERROR DE COMPILACIÓN.
No alteres ningún elemento clasificado como 🟡 PROBLEMA FUNCIONAL O DE CALIDAD.
Si falta un archivo referenciado, créalo con la implementación mínima necesaria para compilar.
Tipo B — Configuración / documentación
Extrae el contenido tal cual, sin modificaciones salvo errores evidentes de sintaxis
(ej: YAML mal indentado).
Tipo C — Excel (.xlsx)
Si viene con contenido real, genera el archivo respetando ese contenido.
Si viene con descripción en lenguaje natural, genera un archivo Excel funcional con:

Fila de encabezados en negrita con color de fondo distintivo
Columnas con ancho ajustado al contenido
Tipos de dato correctos por columna
Validaciones si la descripción lo indica
Hojas nombradas descriptivamente si hay más de una
Filas de ejemplo si no hay datos reales

Tipo D — Word (.docx)
Si viene con contenido real, genera el archivo respetando ese contenido.
Si viene con descripción en lenguaje natural, genera un documento Word funcional con:

Estilos de título (Título 1, Título 2) para jerarquía de secciones
Fuente legible (Calibri o equivalente), tamaño 11-12pt para cuerpo
Márgenes estándar
Tabla de contenido si tiene múltiples secciones
Tablas con encabezados en negrita si aplica

Tipo E — Otro
Genera el archivo con el contenido o estructura más apropiada según la descripción.
PASO 5 — Exportación en ZIP
Empaqueta todos los archivos en un único archivo ZIP descargable respetando exactamente
la estructura de rutas indicada por los marcadores.
El ZIP debe incluir:

Archivos de código con únicamente los errores de compilación corregidos
Archivos de configuración y documentación sin cambios
Archivos nuevos creados para resolver dependencias de compilación faltantes
Archivos Excel y Word generados desde descripción

IMPORTANTE: El ZIP debe estar listo para descargar al finalizar. No preguntes si el usuario
quiere generarlo. Simplemente genera el archivo y proporciona el enlace de descarga; No debes desplegar en el chat el resumen de lo que arreglaste al Zip, solo entregalo.

REGLAS IMPORTANTES

No omitas ningún archivo aunque no tenga errores ni modificaciones
Respeta los nombres y rutas exactas indicadas por los marcadores
Si un archivo no tiene marcador claro, infiere el nombre desde su contenido
Si la cadena contiene solo documentación, placeholders o binarios fake, NO la reproduzcas:
aplicá PASO 0 (materializar el proyecto del briefing). Reproducir la carcasa es un fallo.
No agregues texto después del enlace de descarga del ZIP
No preguntes si el usuario quiere el ZIP: simplemente generalo siempre
Si detectas que falta un archivo de configuración necesario para compilar
(pom.xml, package.json, requirements.txt, build.gradle, etc.), créalo e inclúyelo
inferiendo su contenido desde los imports y frameworks detectados en el código
Nunca corrijas problemas 🟡 aunque parezcan obvios o fáciles de mejorar.
El participante que recibirá este proyecto los debe encontrar y resolver él mismo.


INPUT
Aquí está la cadena con los archivos:

// === ARCHIVO: pyproject.toml ===
[tool.poetry]
name = "streaming-etl-pipeline"
version = "0.1.0"
description = "Pipeline ETL en streaming para procesamiento de datos en tiempo real"
authors = ["Ingeniero de Datos"]
license = "MIT"
readme = "README.md"
packages = [{include = "src"}]

[tool.poetry.dependencies]
python = "^3.13"
pyspark = "3.5.0"
apache-airflow = "2.9.0"
confluent-kafka = "2.3.0"
boto3 = "1.34.0"
pydantic = "2.7.0"
pyyaml = "6.0.1"
pandas = "2.2.0"

[tool.poetry.group.dev.dependencies]
pytest = "8.1.1"

[tool.poetry.scripts]
start-airflow = "airflow standalone"

[build-system]
requires = ["poetry-core>=1.0.0"]
build-backend = "poetry.core.masonry.api"

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-v"

# Configuración para PySpark
[tool.pyspark]
spark_version = "3.5.0"
hadoop_version = "3"

# Configuración para Airflow
[tool.airflow]
dags_folder = "dags"
plugins_folder = "plugins"

// === ARCHIVO: data/schemas/input_schema.json ===
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Esquema de entrada para datos de transacciones",
  "description": "Define la estructura esperada para los datos de transacciones en tiempo real",
  "type": "object",
  "properties": {
    "transaction_id": {
      "description": "Identificador único de la transacción",
      "type": "string",
      "pattern": "^[a-f0-9]{32}$",
      "minLength": 32,
      "maxLength": 32
    },
    "timestamp": {
      "description": "Fecha y hora de la transacción en formato ISO 8601",
      "type": "string",
      "format": "date-time"
    },
    "user_id": {
      "description": "Identificador del usuario",
      "type": "string",
      "minLength": 1,
      "maxLength": 64
    },
    "amount": {
      "description": "Monto de la transacción",
      "type": "number",
      "minimum": 0,
      "exclusiveMinimum": true
    },
    "currency": {
      "description": "Moneda de la transacción",
      "type": "string",
      "enum": ["USD", "EUR", "GBP", "JPY", "MXN"]
    },
    "merchant_id": {
      "description": "Identificador del comercio",
      "type": "string",
      "minLength": 1,
      "maxLength": 64
    },
    "location": {
      "description": "Ubicación geográfica de la transacción",
      "type": "object",
      "properties": {
        "latitude": {
          "type": "number",
          "minimum": -90,
          "maximum": 90
        },
        "longitude": {
          "type": "number",
          "minimum": -180,
          "maximum": 180
        },
        "city": {
          "type": "string",
          "minLength": 1,
          "maxLength": 100
        },
        "country": {
          "type": "string",
          "minLength": 2,
          "maxLength": 2
        }
      },
      "required": ["latitude", "longitude"]
    },
    "payment_method": {
      "description": "Método de pago utilizado",
      "type": "object",
      "properties": {
        "type": {
          "type": "string",
          "enum": ["credit_card", "debit_card", "bank_transfer", "digital_wallet"]
        },
        "details": {
          "type": "object",
          "properties": {
            "card_last_four": {
              "type": "string",
              "pattern": "^[0-9]{4}$"
            },
            "bank_name": {
              "type": "string",
              "minLength": 1,
              "maxLength": 100
            }
          }
        }
      },
      "required": ["type"]
    },
    "metadata": {
      "description": "Metadatos adicionales de la transacción",
      "type": "object",
      "additionalProperties": {
        "type": "string"
      }
    }
  },
  "required": [
    "transaction_id",
    "timestamp",
    "user_id",
    "amount",
    "currency",
    "merchant_id"
  ],
  "additionalProperties": false
}

// === ARCHIVO: dags/streaming_etl_pipeline.py ===
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

// === ARCHIVO: src/extract/kafka_consumer.py ===
"""
Consumidor de datos en streaming desde Apache Kafka.
Maneja buffering, checkpoints, reintentos exponenciales y errores.
"""
import json
import logging
import time
import uuid
from datetime import datetime
from typing import List, Dict, Optional, Any
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaDataConsumer:
    """
    Consumidor de Kafka para ETL en streaming.
    Implementa buffering, manejo de errores y checkpoints para garantizar idempotencia.
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        batch_size: int = 1000,
        timeout_seconds: int = 30,
        max_retries: int = 3,
        checkpoint_enabled: bool = True,
        auto_offset_reset: str = 'latest',
        enable_auto_commit: bool = False
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.batch_size = batch_size
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.checkpoint_enabled = checkpoint_enabled
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        
        self._consumer: Optional[Consumer] = None
        self._processed_offsets: Dict[str, int] = {}
        self._retry_count = 0
        
        self._validate_connection()
        self._init_consumer()
    
    def _validate_connection(self) -> None:
        """Valida la conexión al cluster de Kafka."""
        admin = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        
        try:
            cluster_metadata = admin.list_topics(timeout=10)
            logger.info(f"Conexión a Kafka exitosa. Topics disponibles: {len(cluster_metadata.topics)}")
            
            if self.topic not in cluster_metadata.topics:
                logger.warning(f"Topic '{self.topic}' no existe. Se creará automáticamente.")
        
        except Exception as e:
            logger.error(f"Error validando conexión a Kafka: {str(e)}")
            raise ConnectionError(f"No se puede conectar a Kafka: {str(e)}")
    
    def _init_consumer(self) -> None:
        """Inicializa el consumidor de Kafka con configuración robusta."""
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset,
            'enable.auto.commit': self.enable_auto_commit,
            'auto.commit.interval.ms': 5000,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
            'max.poll.interval.ms': 300000,
            'fetch.min.bytes': 1,
            'fetch.max.wait.ms': 500,
            'enable.partition.eof': False,
            'client.id': f'etl-consumer-{self.group_id}'
        }
        
        self._consumer = Consumer(config)
        self._consumer.subscribe([self.topic])
        
        logger.info(f"Consumidor inicializado para topic '{self.topic}' con group_id '{self.group_id}'")
    
    def _retry_with_backoff(self, operation, *args, **kwargs):
        """Ejecuta operación con reintentos exponenciales."""
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                return operation(*args, **kwargs)
            
            except (KafkaError, KafkaException) as e:
                last_exception = e
                wait_time = (2 ** attempt) * 1.0
                
                logger.warning(f"Intento {attempt + 1}/{self.max_retries} fallido: {str(e)}. "
                             f"Reintentando en {wait_time}s...")
                
                time.sleep(wait_time)
                
                if attempt == self.max_retries - 1:
                    logger.error(f"Todos los reintentos agotados para la operación")
                    raise
        
        raise last_exception
    
    def _process_message(self, msg) -> Optional[Dict[str, Any]]:
        """Procesa un mensaje individual de Kafka."""
        if msg is None:
            return None
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logger.debug("Fin de partición alcanzado")
                return None
            else:
                logger.error(f"Error en mensaje: {msg.error()}")
                return None
        
        try:
            value = msg.value()
            
            if isinstance(value, bytes):
                value = value.decode('utf-8')
            
            payload = json.loads(value)
            
            processed_message = {
                'message_id': self._generate_message_id(msg),
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'timestamp': self._extract_timestamp(msg),
                'payload': payload,
                'key': msg.key().decode('utf-8') if msg.key() else None
            }
            
            return processed_message
        
        except json.JSONDecodeError as e:
            logger.error(f"Error decodificando mensaje JSON: {str(e)}")
            return {
                'message_id': str(uuid.uuid4()),
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'timestamp': datetime.utcnow().isoformat(),
                'payload': {'raw': value.decode('utf-8') if isinstance(value, bytes) else value},
                'parse_error': str(e)
            }
        
        except Exception as e:
            logger.error(f"Error procesando mensaje: {str(e)}")
            return None
    
    def _generate_message_id(self, msg) -> str:
        """Genera identificador único para el mensaje."""
        return f"{msg.topic()}-{msg.partition()}-{msg.offset()}"
    
    def _extract_timestamp(self, msg) -> str:
        """Extrae timestamp del mensaje."""
        if msg.timestamp()[0]:
            ts_type, ts_value = msg.timestamp()
            return datetime.fromtimestamp(ts_value / 1000).isoformat()
        return datetime.utcnow().isoformat()
    
    def _checkpoint_offsets(self, records: List[Dict]) -> None:
        """Guarda offsets procesados para recuperación."""
        if not self.checkpoint_enabled:
            return
        
        for record in records:
            key = f"{record['topic']}-{record['partition']}"
            self._processed_offsets[key] = record['offset']
        
        logger.debug(f"Checkpoints guardados: {len(self._processed_offsets)}")
    
    def consume_batch(self, timeout_seconds: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Consume un lote de mensajes de Kafka.
        
        Args:
            timeout_seconds: Timeout para recibir mensajes. Si es None, usa el valor por defecto.
        
        Returns:
            Lista de mensajes procesados.
        """
        timeout = timeout_seconds or self.timeout_seconds
        batch = []
        start_time = time.time()
        
        logger.info(f"Iniciando consumo de hasta {self.batch_size} mensajes con timeout de {timeout}s")
        
        while len(batch) < self.batch_size:
            remaining_time = timeout - (time.time() - start_time)
            
            if remaining_time <= 0:
                logger.debug("Timeout alcanzado")
                break
            
            msg = self._consumer.poll(timeout=min(remaining_time, 1.0))
            
            processed = self._process_message(msg)
            
            if processed:
                batch.append(processed)
            
            if time.time() - start_time > timeout:
                break
        
        if batch:
            self._checkpoint_offsets(batch)
            logger.info(f"Consumidos {len(batch)} mensajes en {time.time() - start_time:.2f}s")
        else:
            logger.debug("No se recibieron mensajes en esta iteración")
        
        return batch
    
    def consume_single(self) -> Optional[Dict[str, Any]]:
        """Consume un único mensaje de Kafka."""
        msg = self._consumer.poll(timeout=self.timeout_seconds)
        return self._process_message(msg)
    
    def get_consumer_position(self) -> Dict[str, Any]:
        """Obtiene la posición actual del consumidor."""
        assignment = self._consumer.assignment()
        
        positions = {}
        for partition in assignment:
            committed = self._consumer.committed(partition)
            positions[f"{partition.topic}-{partition.partition}"] = {
                'offset': committed.offset() if committed else None,
                'high': self._consumer.get_watermark_offsets(partition)[1] if self._consumer.get_watermark_offsets(partition) else None
            }
        
        return positions
    
    def seek_to_beginning(self) -> None:
        """Reinicia el consumo desde el inicio de cada partición."""
        assignment = self._consumer.assignment()
        self._consumer.seek_to_beginning()
        logger.info("Consumidor reiniciado al inicio de las particiones")
    
    def seek_to_end(self) -> None:
        """Avança el consumo al final de cada partición."""
        assignment = self._consumer.assignment()
        self._consumer.seek_to_end()
        logger.info("Consumidor posicionado al final de las particiones")
    
    def close(self) -> None:
        """Cierra el consumidor y libera recursos."""
        if self._consumer:
            self._consumer.close()
            logger.info("Consumidor de Kafka cerrado")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class KafkaProducerHelper:
    """Productor auxiliar para enviar mensajes de control o errores."""
    
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self._producer = None
    
    def _get_producer(self) -> Producer:
        if self._producer is None:
            config = {
                'bootstrap.servers': self.bootstrap_servers,
                'client.id': 'etl-producer-helper'
            }
            self._producer = Producer(config)
        return self._producer
    
    def send_message(self, topic: str, message: Dict) -> None:
        """Envía un mensaje al topic especificado."""
        producer = self._get_producer()
        
        def delivery_report(err, msg):
            if err:
                logger.error(f"Error enviando mensaje: {err}")
            else:
                logger.debug(f"Mensaje enviado a {msg.topic()} [{msg.partition()}]")
        
        producer.produce(
            topic,
            key=str(message.get('message_id', '')),
            value=json.dumps(message).encode('utf-8'),
            callback=delivery_report
        )
        producer.flush()

// === ARCHIVO: src/extract/s3_reader.py ===
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

// === ARCHIVO: src/transform/data_cleaner.py ===
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

// === ARCHIVO: src/transform/feature_engineering.py ===
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

// === ARCHIVO: src/transform/schema_validator.py ===
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

// === ARCHIVO: src/load/redshift_writer.py ===
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

// === ARCHIVO: src/load/s3_writer.py ===
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

// === ARCHIVO: tests/test_kafka_consumer.py ===
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import time
import json


class TestKafkaConsumer:
    """Pruebas unitarias para el consumidor Kafka del pipeline ETL."""

    @pytest.fixture
    def mock_kafka_config(self):
        """Configuración mock de Kafka para pruebas."""
        return {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 10000,
            'heartbeat.interval.ms': 3000
        }

    @pytest.fixture
    def sample_message(self):
        """Mensaje de ejemplo para pruebas."""
        return {
            'event_id': 'evt-12345',
            'timestamp': '2025-01-15T10:30:00Z',
            'user_id': 'user-987',
            'event_type': 'purchase',
            'amount': 150.00,
            'currency': 'USD',
            'metadata': {
                'source': 'mobile_app',
                'device_id': 'dev-abc123'
            }
        }

    def test_consumer_initialization(self, mock_kafka_config):
        """Verifica que el consumidor se inicializa correctamente."""
        from src.extract.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=mock_kafka_config['bootstrap.servers'],
            group_id=mock_kafka_config['group.id'],
            topics=['test-topic']
        )

        assert consumer.bootstrap_servers == 'localhost:9092'
        assert consumer.group_id == 'test-consumer-group'
        assert 'test-topic' in consumer.topics
        assert consumer.consumer is not None

    def test_consumer_message_parsing(self, mock_kafka_config, sample_message):
        """Verifica el parsing correcto de mensajes Kafka."""
        from src.extract.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=mock_kafka_config['bootstrap.servers'],
            group_id=mock_kafka_config['group.id'],
            topics=['test-topic']
        )

        raw_message = Mock()
        raw_message.value.return_value = json.dumps(sample_message).encode('utf-8')
        raw_message.topic.return_value = 'test-topic'
        raw_message.partition.return_value = 0
        raw_message.offset.return_value = 100

        parsed = consumer.parse_message(raw_message)

        assert parsed['event_id'] == 'evt-12345'
        assert parsed['user_id'] == 'user-987'
        assert parsed['amount'] == 150.00
        assert parsed['timestamp'] == '2025-01-15T10:30:00Z'

    def test_message_processing_latency(self, mock_kafka_config, sample_message):
        """Verifica que el procesamiento cumple con los umbrales de latencia."""
        from src.extract.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=mock_kafka_config['bootstrap.servers'],
            group_id=mock_kafka_config['group.id'],
            topics=['test-topic'],
            max_latency_ms=5000
        )

        message_timestamp = datetime.fromisoformat('2025-01-15T10:30:00Z'.replace('Z', '+00:00'))
        processing_timestamp = datetime.now()

        latency_ms = (processing_timestamp - message_timestamp).total_seconds() * 1000

        assert latency_ms < consumer.max_latency_ms, f"Latency {latency_ms}ms exceeds threshold"

    def test_error_handling_invalid_message(self, mock_kafka_config):
        """Verifica el manejo de errores con mensajes inválidos."""
        from src.extract.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=mock_kafka_config['bootstrap.servers'],
            group_id=mock_kafka_config['group.id'],
            topics=['test-topic']
        )

        invalid_message = Mock()
        invalid_message.value.return_value = b'invalid-json-content'
        invalid_message.topic.return_value = 'test-topic'

        with pytest.raises(json.JSONDecodeError):
            consumer.parse_message(invalid_message)

    def test_offset_commit_on_success(self, mock_kafka_config, sample_message):
        """Verifica que el offset se commitea solo en procesamiento exitoso."""
        from src.extract.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=mock_kafka_config['bootstrap.servers'],
            group_id=mock_kafka_config['group.id'],
            topics=['test-topic']
        )

        consumer.consumer.commit = Mock()

        raw_message = Mock()
        raw_message.value.return_value = json.dumps(sample_message).encode('utf-8')
        raw_message.topic.return_value = 'test-topic'
        raw_message.partition.return_value = 0
        raw_message.offset.return_value = 100

        try:
            consumer.process_message(raw_message)
            consumer.consumer.commit.assert_called_once()
        except Exception:
            pass

    def test_retry_on_transient_error(self, mock_kafka_config, sample_message):
        """Verifica la estrategia de reintento en errores transitorios."""
        from src.extract.kafka_consumer import KafkaConsumer

        max_retries = 3
        consumer = KafkaConsumer(
            bootstrap_servers=mock_kafka_config['bootstrap.servers'],
            group_id=mock_kafka_config['group.id'],
            topics=['test-topic'],
            max_retries=max_retries
        )

        attempt_count = 0

        def mock_process(*args):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < max_retries:
                raise ConnectionError("Transient error")
            return sample_message

        raw_message = Mock()
        raw_message.value.return_value = json.dumps(sample_message).encode('utf-8')

        with patch.object(consumer, 'process_record', side_effect=mock_process):
            result = consumer.process_message(raw_message)
            assert attempt_count == max_retries

    def test_consumer_close_cleanup(self, mock_kafka_config):
        """Verifica la limpieza de recursos al cerrar el consumidor."""
        from src.extract.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=mock_kafka_config['bootstrap.servers'],
            group_id=mock_kafka_config['group.id'],
            topics=['test-topic']
        )

        consumer.consumer.close = Mock()
        consumer.close()

        consumer.consumer.close.assert_called_once()

    def test_batch_processing_performance(self, mock_kafka_config):
        """Verifica el rendimiento del procesamiento por lotes."""
        from src.extract.kafka_consumer import KafkaConsumer

        batch_size = 100
        consumer = KafkaConsumer(
            bootstrap_servers=mock_kafka_config['bootstrap.servers'],
            group_id=mock_kafka_config['group.id'],
            topics=['test-topic'],
            batch_size=batch_size
        )

        messages = []
        for i in range(batch_size):
            msg = Mock()
            msg.value.return_value = json.dumps({'event_id': f'evt-{i}'}).encode('utf-8')
            messages.append(msg)

        start_time = time.time()
        processed = consumer.process_batch(messages)
        elapsed = time.time() - start_time

        assert len(processed) == batch_size
        assert elapsed < 5.0, f"Batch processing took {elapsed}s, exceeds 5s threshold"

// === ARCHIVO: tests/test_schema_validator.py ===
import pytest
from unittest.mock import Mock, patch
import json
from datetime import datetime
from pydantic import ValidationError


class TestSchemaValidator:
    """Pruebas unitarias para el validador de esquemas del pipeline ETL."""

    @pytest.fixture
    def valid_event_schema(self):
        """Esquema de evento válido para pruebas."""
        return {
            'type': 'object',
            'required': ['event_id', 'timestamp', 'user_id', 'event_type'],
            'properties': {
                'event_id': {'type': 'string', 'pattern': '^evt-[0-9]+$'},
                'timestamp': {'type': 'string', 'format': 'date-time'},
                'user_id': {'type': 'string', 'pattern': '^user-[0-9]+$'},
                'event_type': {'type': 'string', 'enum': ['purchase', 'view', 'click', 'signup']},
                'amount': {'type': 'number', 'minimum': 0},
                'currency': {'type': 'string', 'enum': ['USD', 'EUR', 'GBP']},
                'metadata': {'type': 'object'}
            },
            'additionalProperties': False
        }

    @pytest.fixture
    def valid_record(self):
        """Registro válido para pruebas."""
        return {
            'event_id': 'evt-12345',
            'timestamp': '2025-01-15T10:30:00Z',
            'user_id': 'user-987',
            'event_type': 'purchase',
            'amount': 150.00,
            'currency': 'USD',
            'metadata': {'source': 'mobile_app'}
        }

    def test_validator_initialization(self, valid_event_schema):
        """Verifica que el validador se inicializa con el esquema correcto."""
        from src.transform.schema_validator import SchemaValidator

        validator = SchemaValidator(schema=valid_event_schema)

        assert validator.schema == valid_event_schema
        assert validator.quarantine_path is not None

    def test_valid_record_passes_validation(self, valid_event_schema, valid_record):
        """Verifica que un registro válido pasa la validación."""
        from src.transform.schema_validator import SchemaValidator

        validator = SchemaValidator(schema=valid_event_schema)
        result = validator.validate(valid_record)

        assert result.is_valid is True
        assert result.errors is None
        assert result.record == valid_record

    def test_missing_required_field_fails(self, valid_event_schema):
        """Verifica que falta de campo requerido falla la validación."""
        from src.transform.schema_validator import SchemaValidator

        validator = SchemaValidator(schema=valid_event_schema)
        invalid_record = {
            'event_id': 'evt-12345',
            'timestamp': '2025-01-15T10:30:00Z'
        }

        result = validator.validate(invalid_record)

        assert result.is_valid is False
        assert result.errors is not None
        assert 'user_id' in str(result.errors).lower() or 'required' in str(result.errors).lower()

    def test_invalid_field_type_fails(self, valid_event_schema, valid_record):
        """Verifica que tipo de campo inválido falla la validación."""
        from src.transform.schema_validator import SchemaValidator

        validator = SchemaValidator(schema=valid_event_schema)
        invalid_record = valid_record.copy()
        invalid_record['amount'] = 'not-a-number'

        result = validator.validate(invalid_record)

        assert result.is_valid is False
        assert result.errors is not None

    def test_invalid_enum_value_fails(self, valid_event_schema, valid_record):
        """Verifica que valor de enum inválido falla la validación."""
        from src.transform.schema_validator import SchemaValidator

        validator = SchemaValidator(schema=valid_event_schema)
        invalid_record = valid_record.copy()
        invalid_record['event_type'] = 'invalid_type'

        result = validator.validate(invalid_record)

        assert result.is_valid is False

    def test_pattern_mismatch_fails(self, valid_event_schema, valid_record):
        """Verifica que patrón no matching falla la validación."""
        from src.transform.schema_validator import SchemaValidator

        validator = SchemaValidator(schema=valid_event_schema)
        invalid_record = valid_record.copy()
        invalid_record['event_id'] = 'invalid-format-123'

        result = validator.validate(invalid_record)

        assert result.is_valid is False

    def test_additional_properties_rejected(self, valid_event_schema, valid_record):
        """Verifica que propiedades adicionales son rechazadas."""
        from src.transform.schema_validator import SchemaValidator

        validator = SchemaValidator(schema=valid_event_schema)
        invalid_record = valid_record.copy()
        invalid_record['extra_field'] = 'not-allowed'

        result = validator.validate(invalid_record)

        assert result.is_valid is False

    def test_quarantine_invalid_records(self, valid_event_schema):
        """Verifica que registros inválidos se envían a cuarentena."""
        from src.transform.schema_validator import SchemaValidator

        validator = SchemaValidator(schema=valid_event_schema)
        invalid_record = {
            'event_id': 'evt-12345',
            'timestamp': '2025-01-15T10:30:00Z'
        }

        with patch('builtins.open', create=True) as mock_open:
            mock_file = Mock()
            mock_open.return_value.__enter__.return_value = mock_file

            result = validator.validate(invalid_record, quarantine=True)

            assert result.is_valid is False
            mock_file.write.assert_called()

    def test_batch_validation(self, valid_event_schema, valid_record):
        """Verifica la validación por lotes."""
        from src.transform.schema_validator import SchemaValidator

        validator = SchemaValidator(schema=valid_event_schema)

        records = [valid_record.copy() for _ in range(10)]
        records[5]['event_type'] = 'invalid_type'

        results = validator.validate_batch(records)

        valid_count = sum(1 for r in results if r.is_valid)
        invalid_count = sum(1 for r in results if not r.is_valid)

        assert valid_count == 9
        assert invalid_count == 1

    def test_validation_performance(self, valid_event_schema, valid_record):
        """Verifica el rendimiento de la validación."""
        from src.transform.schema_validator import SchemaValidator
        import time

        validator = SchemaValidator(schema=valid_event_schema)

        records = [valid_record.copy() for _ in range(1000)]

        start_time = time.time()
        results = validator.validate_batch(records)
        elapsed = time.time() - start_time

        assert all(r.is_valid for r in results)
        assert elapsed < 2.0, f"Validation took {elapsed}s, exceeds 2s threshold"

    def test_schema_evolution_compatibility(self, valid_event_schema):
        """Verifica el manejo de evolución de esquemas."""
        from src.transform.schema_validator import SchemaValidator

        old_schema = valid_event_schema.copy()
        new_schema = valid_event_schema.copy()
        new_schema['properties']['new_field'] = {'type': 'string'}

        validator = SchemaValidator(schema=old_schema, allow_new_fields=True)

        record_with_new_field = {
            'event_id': 'evt-12345',
            'timestamp': '2025-01-15T10:30:00Z',
            'user_id': 'user-987',
            'event_type': 'purchase',
            'new_field': 'value'
        }

        result = validator.validate(record_with_new_field)

        assert result.is_valid is True

// === ARCHIVO: tests/test_redshift_writer.py ===
import pytest
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime
import psycopg2
import json


class TestRedshiftWriter:
    """Pruebas unitarias para el escritor de Redshift del pipeline ETL."""

    @pytest.fixture
    def redshift_config(self):
        """Configuración de Redshift para pruebas."""
        return {
            'host': 'test-cluster.xxxx.us-east-1.redshift.amazonaws.com',
            'port': 5439,
            'database': 'analytics',
            'username': 'admin',
            'password': 'test_password',
            'schema': 'public'
        }

    @pytest.fixture
    def sample_records(self):
        """Registros de ejemplo para pruebas."""
        return [
            {
                'event_id': 'evt-12345',
                'timestamp': '2025-01-15T10:30:00Z',
                'user_id': 'user-987',
                'event_type': 'purchase',
                'amount': 150.00,
                'currency': 'USD'
            },
            {
                'event_id': 'evt-12346',
                'timestamp': '2025-01-15T10:31:00Z',
                'user_id': 'user-988',
                'event_type': 'view',
                'amount': None,
                'currency': None
            }
        ]

    @pytest.fixture
    def mock_connection(self):
        """Mock de conexión a Redshift."""
        with patch('psycopg2.connect') as mock_connect:
            conn = MagicMock()
            mock_connect.return_value = conn
            yield conn

    def test_writer_initialization(self, redshift_config):
        """Verifica que el escritor se inicializa correctamente."""
        from src.load.redshift_writer import RedshiftWriter

        writer = RedshiftWriter(
            host=redshift_config['host'],
            port=redshift_config['port'],
            database=redshift_config['database'],
            username=redshift_config['username'],
            password=redshift_config['password'],
            schema=redshift_config['schema']
        )

        assert writer.host == redshift_config['host']
        assert writer.port == redshift_config['port']
        assert writer.database == redshift_config['database']
        assert writer.schema == redshift_config['schema']

    def test_successful_write(self, redshift_config, sample_records, mock_connection):
        """Verifica escritura exitosa de registros."""
        from src.load.redshift_writer import RedshiftWriter

        writer = RedshiftWriter(
            host=redshift_config['host'],
            port=redshift_config['port'],
            database=redshift_config['database'],
            username=redshift_config['username'],
            password=redshift_config['password'],
            schema=redshift_config['schema']
        )

        cursor = MagicMock()
        mock_connection.cursor.return_value = cursor

        result = writer.write_records('events', sample_records)

        assert result.success is True
        assert result.records_written == len(sample_records)
        cursor.execute.assert_called()
        mock_connection.commit.assert_called_once()

    def test_transaction_rollback_on_error(self, redshift_config, sample_records, mock_connection):
        """Verifica rollback de transacción en caso de error."""
        from src.load.redshift_writer import RedshiftWriter

        writer = RedshiftWriter(
            host=redshift_config['host'],
            port=redshift_config['port'],
            database=redshift_config['database'],
            username=redshift_config['username'],
            password=redshift_config['password'],
            schema=redshift_config['schema']
        )

        cursor = MagicMock()
        cursor.execute.side_effect = Exception("Database error")
        mock_connection.cursor.return_value = cursor

        result = writer.write_records('events', sample_records)

        assert result.success is False
        mock_connection.rollback.assert_called_once()

    def test_idempotency_check(self, redshift_config, sample_records, mock_connection):
        """Verifica la verificación de idempotencia antes de escribir."""
        from src.load.redshift_writer import RedshiftWriter

        writer = RedshiftWriter(
            host=redshift_config['host'],
            port=redshift_config['port'],
            database=redshift_config['database'],
            username=redshift_config['username'],
            password=redshift_config['password'],
            schema=redshift_config['schema']
        )

        cursor = MagicMock()
        cursor.fetchone.return_value = (1,)
        mock_connection.cursor.return_value = cursor

        result = writer.write_records('events', sample_records, check_idempotency=True)

        assert result.already_exists is True
        assert result.records_written == 0

    def test_batch_size_handling(self, redshift_config, sample_records, mock_connection):
        """Verifica el manejo correcto del tamaño de lote."""
        from src.load.redshift_writer import RedshiftWriter

        writer = RedshiftWriter(
            host=redshift_config['host'],
            port=redshift_config['port'],
            database=redshift_config['database'],
            username=redshift_config['username'],
            password=redshift_config['password'],
            schema=redshift_config['schema'],
            batch_size=1
        )

        cursor = MagicMock()
        mock_connection.cursor.return_value = cursor

        large_batch = sample_records * 50
        result = writer.write_records('events', large_batch)

        assert cursor.execute.call_count > 1

    def test_retry_on_connection_failure(self, redshift_config, sample_records):
        """Verifica la estrategia de reintento en fallos de conexión."""
        from src.load.redshift_writer import RedshiftWriter

        with patch('psycopg2.connect', side_effect=[Exception("Connection failed"), Exception("Connection failed"), MagicMock()]) as mock_connect:
            writer = RedshiftWriter(
                host=redshift_config['host'],
                port=redshift_config['port'],
                database=redshift_config['database'],
                username=redshift_config['username'],
                password=redshift_config['password'],
                schema=redshift_config['schema'],
                max_retries=3
            )

            cursor = MagicMock()
            mock_connect.return_value.cursor.return_value = cursor

            result = writer.write_records('events', sample_records)

            assert mock_connect.call_count == 3

    def test_consistency_verification(self, redshift_config, sample_records, mock_connection):
        """Verifica la consistencia de datos después de la escritura."""
        from src.load.redshift_writer import RedshiftWriter

        writer = RedshiftWriter(
            host=redshift_config['host'],
            port=redshift_config['port'],
            database=redshift_config['database'],
            username=redshift_config['username'],
            password=redshift_config['password'],
            schema=redshift_config['schema']
        )

        cursor = MagicMock()
        mock_connection.cursor.return_value = cursor
        cursor.fetchone.return_value = (len(sample_records),)

        writer.write_records('events', sample_records)

        verify_cursor = MagicMock()
        verify_cursor.fetchone.return_value = (len(sample_records),)
        mock_connection.cursor.return_value = verify_cursor

        count = writer.verify_record_count('events')

        assert count == len(sample_records)

    def test_partition_key_extraction(self, redshift_config, sample_records, mock_connection):
        """Verifica la extracción de clave de partición para Redshift."""
        from src.load.redshift_writer import RedshiftWriter

        writer = RedshiftWriter(
            host=redshift_config['host'],
            port=redshift_config['port'],
            database=redshift_config['database'],
            username=redshift_config['username'],
            password=redshift_config['password'],
            schema=redshift_config['schema'],
            partition_column='timestamp'
        )

        cursor = MagicMock()
        mock_connection.cursor.return_value = cursor

        writer.write_records('events', sample_records)

        calls = cursor.execute.call_args_list
        assert any('2025-01-15' in str(call) for call in calls)

    def test_dead_letter_queue_on_failure(self, redshift_config, sample_records, mock_connection):
        """Verifica el manejo de cola de mensajes fallidos."""
        from src.load.redshift_writer import RedshiftWriter

        writer = RedshiftWriter(
            host=redshift_config['host'],
            port=redshift_config['port'],
            database=redshift_config['database'],
            username=redshift_config['username'],
            password=redshift_config['password'],
            schema=redshift_config['schema'],
            dead_letter_queue='s3://bucket/dead-letter/'
        )

        cursor = MagicMock()
        cursor.execute.side_effect = Exception("Validation error")
        mock_connection.cursor.return_value = cursor

        with patch('src.load.redshift_writer.s3_writer') as mock_s3:
            mock_s3.write_batch.return_value = True

            result = writer.write_records('events', sample_records, use_dead_letter=True)

            assert result.success is False
            assert result.sent_to_dlq == len(sample_records)

    def test_connection_close_cleanup(self, redshift_config, sample_records, mock_connection):
        """Verifica la limpieza de recursos al cerrar conexión."""
        from src.load.redshift_writer import RedshiftWriter

        writer = RedshiftWriter(
            host=redshift_config['host'],
            port=redshift_config['port'],
            database=redshift_config['database'],
            username=redshift_config['username'],
            password=redshift_config['password'],
            schema=redshift_config['schema']
        )

        writer.close()

        mock_connection.close.assert_called_once()

// === ARCHIVO: conf/config.yaml ===
environment: development
pipeline:
  name: streaming-etl-pipeline
  version: 1.0.0
  description: Pipeline ETL en streaming para procesamiento de datos en tiempo real

spark:
  app_name: StreamingETL
  master: local[*]
  memory: 4g
  cores: 2
  checkpoint_dir: s3a://data-lake/checkpoints/
  max_offsets_per_trigger: 10000
  watermark_delay_threshold: 10 minutes
  trigger_interval: 30 seconds

kafka:
  bootstrap_servers:
    dev: localhost:9092
    prod: kafka-prod-1:9092,kafka-prod-2:9092,kafka-prod-3:9092
  consumer_group: etl-streaming-consumer
  topics:
    - raw_events
    - user_activities
    - sensor_data
  security:
    protocol: SSL
    mechanism: PLAIN
  offsets:
    auto_reset: earliest
    commit_interval_ms: 5000

s3:
  bucket: data-lake-raw
  region: us-east-1
  format: parquet
  compression: snappy
  partition_by:
    - year
    - month
    - day
    - hour
  path_prefix: raw/
  staging_path: staging/
  output_path: processed/

redshift:
  host: redshift-dev.cluster-xyz.us-east-1.redshift.amazonaws.com
  port: 5439
  database: analytics
  schema: public
  user: awsuser
  iam_role: arn:aws:iam::123456789012:role/RedshiftS3ReadWrite
  table: streaming_events
  distkey: event_timestamp
  sortkey: event_date
  max_batch_size: 10000
  copy_options:
    format: parquet
    json: auto
    acceptinvchars: as '_'
    emptyasnull: true
    blanksasnull: true

quality:
  enabled: true
  quarantine_enabled: true
  quarantine_path: s3a://data-lake/quarantine/
  rules:
    - name: null_check
      column: event_id
      check: is_not_null
      action: quarantine
      threshold: 0
    - name: timestamp_valid
      column: event_timestamp
      check: is_valid_timestamp
      action: quarantine
      threshold: 0
    - name: value_range
      column: amount
      check: range
      min: 0
      max: 1000000
      action: quarantine
      threshold: 0.05
    - name: string_length
      column: user_id
      check: length
      min: 1
      max: 100
      action: quarantine
      threshold: 0
  metrics:
    record_count: true
    null_percentage: true
    duplicate_check: true
    schema_compliance: true

retry:
  max_attempts: 3
  backoff_multiplier: 2
  initial_delay_seconds: 1
  max_delay_seconds: 60
  retry_on_errors:
    - ConnectionError
    - TimeoutError
    - TransientError

monitoring:
  enable_metrics: true
  metrics_backend: cloudwatch
  log_level: INFO
  alert_on_failure: true
  alert_email: data-team@company.com
  dashboard_url: https://console.aws.amazon.com/cloudwatch/dashboard

airflow:
  dag_schedule: 0 * * * *  # Cada hora
  dag_timeout: 3600
  max_active_runs: 1
  catchup: false
  concurrency: 4
  pool: etl_pool
  execution_timeout: 7200
  depends_on_past: false

// === ARCHIVO: README.md ===
# Streaming ETL Pipeline

Pipeline de ETL en streaming para procesamiento de datos en tiempo real utilizando Apache Airflow y PySpark.

## Arquitectura

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Kafka     │────▶│   Extract   │────▶│  Transform  │────▶│    Load     │
│  (Source)   │     │  (Consumer) │     │  (PySpark)  │     │ (S3/Redshift)│
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                           │                   │                   │
                           ▼                   ▼                   ▼
                    ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
                    │  Schema     │     │   Data      │     │   Quality   │
                    │  Validation │     │   Cleaner   │     │   Rules     │
                    └─────────────┘     └─────────────┘     └─────────────┘
```

## Estructura del Proyecto

```
streaming-etl-pipeline/
├── conf/
│   └── config.yaml              # Configuración por ambiente
├── data/
│   └── schemas/
│       └── input_schema.json    # Esquemas de validación
├── dags/
│   └── streaming_etl_pipeline.py  # Orquestación Airflow
├── src/
│   ├── extract/
│   │   ├── kafka_consumer.py    # Consumidor de Kafka
│   │   └── s3_reader.py         # Lector de S3
│   ├── transform/
│   │   ├── data_cleaner.py      # Limpieza de datos
│   │   ├── feature_engineering.py  # Generación de features
│   │   └── schema_validator.py  # Validación de esquemas
│   └── load/
│       ├── redshift_writer.py   # Escritor a Redshift
│       └── s3_writer.py         # Escritor a S3
├── tests/
│   ├── test_kafka_consumer.py   # Tests consumidor Kafka
│   ├── test_schema_validator.py # Tests validación
│   └── test_redshift_writer.py  # Tests escritor Redshift
├── pyproject.toml               # Dependencias del proyecto
└── README.md                    # Este archivo
```

## Requisitos

- Python 3.13+
- Poetry (gestor de dependencias)
- Acceso a AWS (S3, Redshift, CloudWatch)
- Apache Airflow 2.9.0
- Apache Spark 3.5.0
- Apache Kafka

## Instalación

1. Clonar el repositorio:

```bash
git clone <repo-url>
cd streaming-etl-pipeline
```

2. Instalar dependencias con Poetry:

```bash
poetry install
```

3. Configurar variables de entorno:

```bash
cp .env.example .env
# Editar .env con las credenciales correspondientes
```

4. Verificar la instalación:

```bash
poetry run pytest -v
```

## Configuración

El archivo `conf/config.yaml` contiene la configuración del pipeline para diferentes ambientes:

### Desarrollo

- Kafka: `localhost:9092`
- Redshift: cluster de desarrollo
- S3: bucket de desarrollo
- Modo debug habilitado

### Producción

- Kafka: cluster de producción con SSL
- Redshift: cluster de producción
- S3: bucket de producción
- Logging avanzado y monitoreo completo

### Parámetros Principales

| Sección | Parámetro | Descripción |
|---------|-----------|-------------|
| spark | max_offsets_per_trigger | Máximos offsets por trigger |
| spark | trigger_interval | Intervalo de ejecución |
| kafka | consumer_group | Grupo de consumidores |
| s3 | format | Formato de archivo (parquet) |
| s3 | partition_by | Particiones por fecha/hora |
| redshift | max_batch_size | Tamaño máximo de batch |
| quality | quarantine_enabled | Habilitar cuarentena |
| retry | max_attempts | Intentos máximos de reintento |

## Ejecución

### Ejecución Local con Airflow

```bash
# Iniciar Airflow
poetry run airflow standalone

# O iniciar solo el scheduler
poetry run airflow scheduler
```

### Ejecución del Pipeline Directamente

```bash
# Ejecutar el DAG manualmente
poetry run python -m dags.streaming_etl_pipeline

# O ejecutar con Spark
spark-submit --master local[*] src/extract/kafka_consumer.py
```

### Ejecución de Tests

```bash
# Todos los tests
poetry run pytest

# Tests específicos
poetry run pytest tests/test_kafka_consumer.py -v
poetry run pytest tests/test_schema_validator.py -v
poetry run pytest tests/test_redshift_writer.py -v

# Coverage
poetry run pytest --cov=src --cov-report=html
```

## Componentes del Pipeline

### Ingesta (Extract)

El componente de ingesta se encarga de recibir datos desde las fuentes:

- **kafka_consumer.py**: Consume mensajes de Apache Kafka en tiempo real
- **s3_reader.py**: Lee datos históricos desde S3 para backfill

### Transformación (Transform)

Aplica reglas de negocio y transforma los datos:

- **schema_validator.py**: Valida que los datos cumplan el esquema esperado
- **data_cleaner.py**: Limpia y normaliza datos
- **feature_engineering.py**: Genera features derivados

### Carga (Load)

Persiste los datos transformados:

- **s3_writer.py**: Escribe en S3 en formato Parquet particionado
- **redshift_writer.py**: Carga datos en Redshift usando COPY

## Calidad de Datos

El pipeline implementa validación de calidad en múltiples niveles:

1. **Validación de esquema**: Verifica tipos y estructura de datos
2. **Validación de valores**: Verifica rangos y formatos
3. **Cuarentena**: Registros que no pasan las validaciones se aíslan
4. **Métricas**: Monitoreo de volumen, nulls y duplicados

Los registros en cuarentena se almacenan en `s3://data-lake/quarantine/` para revisión manual.

## Monitoreo

- **CloudWatch**: Métricas de pipeline y logs
- **Airflow UI**: Estado de ejecuciones y tareas
- **Alertas**: Notificaciones por email en fallos

## Patrones Implementados

- **Idempotencia**: Cada mensaje se procesa exactamente una vez usando offsets
- **Particionamiento**: Datos organizados por año/mes/día/hora
- **Formato columnar**: Parquet con compresión Snappy
- **Reintentos**: Backoff exponencial en fallos transitorios
- **Esquemas evolutivos**: Compatibilidad hacia atrás con esquemas

## Desarrollo

### Agregar nueva fuente de datos

1. Agregar configuración en `conf/config.yaml`
2. Crear nuevo consumidor en `src/extract/`
3. Agregar tests en `tests/`

### Agregar nueva transformación

1. Crear función en `src/transform/`
2. Registrar en el DAG de Airflow
3. Agregar casos de prueba

### Agregar nuevo destino

1. Agregar configuración en `conf/config.yaml`
2. Crear escritor en `src/load/`
3. Agregar tests de integración

## Licencia

MIT
```
