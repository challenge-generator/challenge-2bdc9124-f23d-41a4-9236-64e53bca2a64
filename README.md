# Implementación de ETL en Streaming

La empresa necesita procesar en tiempo real grandes volúmenes de datos estructurados, semiestructurados y no estructurados provenientes de múltiples fuentes. El sistema debe integrarse con proveedores de nube para alojar los componentes de procesamiento. El objetivo es diseñar y desarrollar un pipeline de ETL que garantice la consistencia, latencia y escalabilidad requeridas por el negocio.

## Informacion General

| Campo | Valor |
|-------|-------|
| **Tema** | Procesamiento de datos en Streaming |
| **Nivel** | senior-l2 |
| **Tipo** | practical |
| **Tiempo estimado** | 4 semanas |

## Fases del Reto

### Fase 0: Configuración del Proyecto

**Objetivo:** Obtener el proyecto base funcional enviando el Código Base a un asistente de IA, que lo analizará, corregirá errores y generará un ZIP listo para usar.

**Tiempo estimado:** 15-30 minutos

**Instrucciones:**

- Asegúrate de tener instalado para ejecutar el proyecto: Un IDE o editor de código.
- Copia todo el contenido del campo **Código Base** de este reto — incluyendo el texto de instrucciones que aparece al inicio.
- Abre un asistente de IA (Claude en claude.ai, ChatGPT o Gemini — se recomienda Claude), pega el contenido copiado en el chat y envíalo.
- El asistente analizará los archivos, corregirá errores y generará un archivo ZIP descargable. Descárgalo y extráelo en la carpeta donde quieras trabajar.
- Verifica que el proyecto arranca sin errores.

**Entregable:** El proyecto compila/arranca sin errores.

<details>
<summary>Pistas de conocimiento</summary>

- Copia el Código Base completo incluyendo el texto de instrucciones al inicio — esas instrucciones le indican al asistente exactamente qué hacer con los archivos.
- Si el asistente no genera el ZIP automáticamente al terminar el análisis, escríbele: "genera el ZIP ahora".
- Si el proyecto tiene errores al arrancar, comparte el mensaje de error con el mismo asistente para que lo corrija.

</details>

### Fase 1: Diseño del pipeline de ETL

**Objetivo:** Definir la arquitectura del pipeline de ETL, identificando las fuentes de datos, los componentes necesarios y las restricciones del dominio.

**Tiempo estimado:** 1 semana

**Instrucciones:**

- Identificar las fuentes de datos y sus características (tipo, volumen, frecuencia).
- Definir los componentes del pipeline (ingestion, transformation, load) y sus responsabilidades.
- Establecer las restricciones del dominio (latencia máxima, consistencia, escalabilidad).

**Entregable:** Documento de diseño del pipeline de ETL.

<details>
<summary>Pistas de conocimiento</summary>

- Considera la latencia máxima permitida para el procesamiento de datos.
- Evalúa la consistencia necesaria entre los datos procesados y la fuente original.

</details>

### Fase 2: Implementación de la ingesta de datos

**Objetivo:** Desarrollar el componente de ingesta que reciba y procese los datos en tiempo real desde las fuentes identificadas.

**Tiempo estimado:** 1 semana

**Instrucciones:**

- Implementar la conexión con las fuentes de datos y el procesamiento inicial.
- Asegurar que los datos se ingresen en el pipeline con la latencia y consistencia requeridas.

**Entregable:** Componente de ingesta de datos funcional.

<details>
<summary>Pistas de conocimiento</summary>

- Usa técnicas de buffering para manejar picos de carga.
- Implementa mecanismos de reintento para manejar fallos transitorios.

</details>

### Fase 3: Transformación de datos

**Objetivo:** Desarrollar el componente de transformación que aplique las reglas de negocio y convierta los datos en el formato requerido.

**Tiempo estimado:** 1 semana

**Instrucciones:**

- Implementar las reglas de negocio para transformar los datos.
- Asegurar que los datos transformados cumplan con los requisitos de consistencia y escalabilidad.

**Entregable:** Componente de transformación de datos funcional.

<details>
<summary>Pistas de conocimiento</summary>

- Usa técnicas de particionamiento para mejorar la escalabilidad.
- Implementa validaciones para asegurar la consistencia de los datos transformados.

</details>

### Fase 4: Carga de datos

**Objetivo:** Desarrollar el componente de carga que persista los datos transformados en el destino final.

**Tiempo estimado:** 1 semana

**Instrucciones:**

- Implementar la persistencia de los datos transformados en el destino final.
- Asegurar que los datos se carguen con la latencia y consistencia requeridas.

**Entregable:** Componente de carga de datos funcional.

<details>
<summary>Pistas de conocimiento</summary>

- Usa técnicas de batch para mejorar la eficiencia de la carga.
- Implementa mecanismos de recuperación para manejar fallos en la persistencia de datos.

</details>

## Dimensiones Evaluadas

- **queEs**: ¿Qué es un pipeline de ETL y cuáles son sus componentes?
- **paraQueSirve**: ¿Para qué sirve un pipeline de ETL en el procesamiento de datos en streaming?
- **comoSeUsa**: ¿Cómo se usa un pipeline de ETL para procesar datos en tiempo real?
- **erroresComunes**: ¿Cuáles son los errores comunes en la implementación de un pipeline de ETL y cómo se pueden evitar?
- **queDecisionesImplica**: ¿Qué decisiones implica el diseño y desarrollo de un pipeline de ETL en streaming?

## Criterios de Evaluacion

- Diseñar un pipeline de ETL que cumpla con las restricciones del dominio.
- Implementar la ingesta de datos con latencia y consistencia requeridas.
- Aplicar reglas de negocio en la transformación de datos.
- Persistir los datos transformados con latencia y consistencia requeridas.

## Como trabajar con un asistente de IA

Hay dos caminos, elegi uno:

- **AGENTS.md** (recomendado) — instrucciones nativas del repo. Abri esta carpeta con tu agente local (Claude Code, Cursor, Codex, Copilot, Gemini) y las carga solo. Sabe que archivos faltan y con que comando se verifica, y completa el scaffold escribiendo en disco.
- **PROMPT_MEJORA.md** — para copiar y pegar en un chat (claude.ai, ChatGPT). Devuelve un ZIP con el proyecto. Sirve si no tenes un agente en el IDE.

Ninguno de los dos resuelve las fases del reto: eso es tu trabajo.

## Verificacion

El proyecto esta listo para trabajar cuando este comando corre sin errores:

```bash
pip install -r requirements.txt && pytest -q
```

---

*Reto generado automaticamente por Challenge Generator - Pragma*
