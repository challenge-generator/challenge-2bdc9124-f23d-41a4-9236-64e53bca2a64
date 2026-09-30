# AGENTS.md

Instrucciones para el agente de IA que abra este repositorio (Claude Code, Cursor, Codex, Copilot, Gemini). Se cargan solas: no hay que pegar nada en ningun chat.

## Que es este repositorio

Es el codigo base de un reto de aprendizaje de Pragma: **Implementación de ETL en Streaming**.

| | |
|---|---|
| Tema | Procesamiento de datos en Streaming |
| Nivel | senior-l2 |
| Chapter | Ciencia de Datos — Ingeniero de Datos |
| Especialidad | Ingeniero de datos |
| Stack | Python 3.13 / Apache Airflow + PySpark |
| Patron arquitectonico | Pipeline ETL en Streaming con capas separadas (ingestion, transformation, load) y orquestación con Airflow |
| Tiempo estimado | 4 semanas |

## Receta del stack

Esqueleto obligatorio:

- `pyproject.toml o requirements.txt en la raiz`
- `dags/ con el DAG de Airflow o el orquestador equivalente`
- `src/extract con los lectores de origen`
- `src/transform con las transformaciones y las reglas de calidad`
- `src/load con los escritores de destino`
- `tests/ con casos de validacion de resultados esperados`
- `conf/ con la configuracion por ambiente`

Dependencias:

- pyspark 3.5.0
- apache-airflow 2.9.0
- confluent-kafka 2.3.0
- boto3 1.34.0
- pydantic 2.7.0
- pytest 8.1.1
- pyyaml 6.0.1
- pandas 2.2.0

## Tu tarea

Dejar este proyecto en estado **verificable**: que el comando de verificacion corra sin errores. Escribi los archivos en disco, en este repositorio. No generes ZIPs ni archivos adjuntos.

En orden:

1. Corre `pip install -r requirements.txt && pytest -q` y mira que falla.
2. Completa lo que falte de la lista de abajo: manifiesto de dependencias, punto de entrada, capa de interfaz y las capas del patron declarado.
3. Arregla SOLO los errores que impiden compilar o arrancar.
4. Volve a correr `pip install -r requirements.txt && pytest -q` hasta que pase.
5. Pará ahí.

## Regla dura: las fases son trabajo del humano

**PROHIBIDO implementar los entregables de las fases.** El valor del reto esta en que la persona los resuelva. Tu trabajo es que tenga un proyecto que arranca; el hueco pedagogico se queda como esta.

No resuelvas nada de esto:

- **Fase 1 — Diseño del pipeline de ETL**: Documento de diseño del pipeline de ETL.
- **Fase 2 — Implementación de la ingesta de datos**: Componente de ingesta de datos funcional.
- **Fase 3 — Transformación de datos**: Componente de transformación de datos funcional.
- **Fase 4 — Carga de datos**: Componente de carga de datos funcional.

Distincion operativa:

- **Arreglar** (si): import faltante, tipo que no existe, dependencia sin declarar, error de sintaxis, archivo referenciado que no existe.
- **No tocar** (no): logica de negocio incompleta, validaciones ausentes, secretos hardcodeados, APIs deprecadas que funcionan, concurrencia insegura, patrones mejorables. Eso es lo que la persona tiene que encontrar.

## Lo que falta y tenes que completar

### 1. Referencias colgando (6)

Salieron de un analisis estatico del codigo que SI esta en el repo. Cada una rompe la compilacion:

- [ ] `dags/streaming_etl_pipeline.py` — `FeatureEngineer.xcom_push`
      Se invoca `xcom_push` sobre `FeatureEngineer`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- [ ] `dags/streaming_etl_pipeline.py` — `FeatureEngineer.xcom_pull`
      Se invoca `xcom_pull` sobre `FeatureEngineer`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- [ ] `tests/test_schema_validator.py` — `SchemaValidator.validate_batch`
      Se invoca `validate_batch` sobre `SchemaValidator`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- [ ] `tests/test_redshift_writer.py` — `RedshiftWriter.write_records`
      Se invoca `write_records` sobre `RedshiftWriter`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- [ ] `tests/test_redshift_writer.py` — `RedshiftWriter.verify_record_count`
      Se invoca `verify_record_count` sobre `RedshiftWriter`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- [ ] `tests/test_redshift_writer.py` — `RedshiftWriter.close`
      Se invoca `close` sobre `RedshiftWriter`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.

### Presentes (15)

- `pyproject.toml`
- `data/schemas/input_schema.json`
- `dags/streaming_etl_pipeline.py`
- `src/extract/kafka_consumer.py`
- `src/extract/s3_reader.py`
- `src/transform/data_cleaner.py`
- `src/transform/feature_engineering.py`
- `src/transform/schema_validator.py`
- `src/load/redshift_writer.py`
- `src/load/s3_writer.py`
- `tests/test_kafka_consumer.py`
- `tests/test_schema_validator.py`
- `tests/test_redshift_writer.py`
- `conf/config.yaml`
- `README.md`

### Capas del patron declarado

Cada una tiene que existir como directorio real con al menos un archivo. Codigo plano en la raiz no satisface el patron.

- `dags`
- `src/extract`
- `src/transform`
- `src/load`
- `tests`
- `conf`
- `data/schemas`

## Verificacion

```bash
pip install -r requirements.txt && pytest -q
```

Ese comando pasando es la definicion de "terminado" para vos.

## Convenciones que tenes que respetar

- Un solo ecosistema: no declares librerias de otro lenguaje ni mezcles gestores de paquetes.
- Toda libreria que uses tiene que estar declarada en el manifiesto de dependencias.
- Todo import declarado tiene que usarse; todo tipo usado tiene que existir o venir de una dependencia declarada.
- El patron es **Pipeline ETL en Streaming con capas separadas (ingestion, transformation, load) y orquestación con Airflow**: los contratos (interfaces, puertos) los define la capa interna y los implementa la externa, nunca al revés.
- Los archivos que crees llevan implementacion real, no stubs: sin `TODO`, sin cuerpos vacios, sin `// getters y setters`.

## Contexto del candidato

Sirve para calibrar el nivel del codigo, no para resolver las fases.

- Perfil: Chapter Ciencia de Datos, Especialidad Ingeniero de Datos, Tecnología Streaming, Senior
- Brecha que el reto ataca: Implementar ETLs de mayor complejidad para procesar datos estructurados, semiestructurados y no estructurados con componentes alojados en proveedores de nube
- Mision: Candidato con experiencia en procesamiento de datos, familiarizado con arquitecturas en la nube

---

*Generado por Challenge Generator — Pragma. `README.md` tiene el enunciado completo del reto para la persona. `PROMPT_MEJORA.md` es la variante para pegar en un chat, si se prefiere ese flujo.*
