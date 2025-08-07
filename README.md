# M2 Integrador - Pipeline ETL con Docker y Airflow

¡Bienvenido al proyecto **M2 Integrador**!  
Este repositorio contiene la implementación de un pipeline **ETL** (Extracción, Transformación y Carga) para un conjunto de datos de **Airbnb en Nueva York**.

El proyecto está diseñado para ser **robusto**, **escalable** y **reproducible**, utilizando tecnologías de contenedores como **Docker** y orquestación con **Apache Airflow**.

---

## 🗂️ Estructura del Proyecto

La estructura del repositorio se organiza de la siguiente manera:

### `Avance 1`
Contiene la documentación del proyecto, incluyendo un archivo de arquitectura que describe el diseño del pipeline y las decisiones técnicas.

### `Avance2_3_4`
Contiene todo el código fuente del pipeline ETL:

- `dags/` - Archivos DAG de Airflow para orquestar el flujo de trabajo.
- `data_sources/` - Ubicación del archivo de datos CSV original.
- `raw_data/` - Aca van a estar los datos extraídos y procesados en la primera etapa.
- `scripts/` - Scripts de Python para la ingesta y transformación.
- `docker-compose.yml` - Archivo principal para levantar el proyecto con Docker y Airflow.
- `docker-compose_local.yml` - Versión alternativa para un entorno de desarrollo local.
- `Dockerfile.*` - Archivos de Docker para construir las imágenes de los servicios.
- `test_pipeline.py` - Script para validar que los datos se han procesado correctamente.

---

## 🚀 Cómo levantar el proyecto

La forma recomendada de ejecutar el pipeline es a través de **Airflow**, que permite visualizar y gestionar las tareas.

### ✅ Con Airflow y Docker Compose

1. **Levantar el entorno de Docker**

   Asegúrate de tener **Docker** y **Docker Compose** instalados.  
   Desde la carpeta `Avance2_3_4`, ejecutá:

   ```bash
   docker-compose -f docker-compose.yml up -d
   ```
2. **Acceder a la interfaz de Airflow**

    Una vez que los contenedores estén listos, accedé a: http://localhost:8080

3. **Ejecutar el DAG**
    
    - Iniciá sesión con usuario: admin y contraseña: admin

    - En la interfaz, buscá el DAG llamado etl_pipeline_docker y activalo.

    - Dispará una nueva ejecución (Trigger) del DAG para que se ejecute el pipeline ETL completo.

## 🔄 Flujo de Trabajo de CI/CD

El proyecto utiliza GitHub Actions para automatizar el pipeline de CI/CD.
Este flujo se ejecuta en cada push o pull_request a la rama main y realiza las siguientes tareas:

Build 🛠️: Construcción de las imágenes Docker.

Test 🧪: Ejecución de tareas de ingesta y transformación dentro de contenedores.

Validation ✅: Verificación de que los datos fueron procesados correctamente y los archivos de salida existen en las capas Silver y Gold.

## Cómo usar el Jupyter Notebook para análisis

Una vez que el pipeline ETL ha terminado y los datos se han guardado en la capa Gold, puedes usar el contenedor de Jupyter para realizar análisis y crear dashboards.

Levantar el entorno:
Asegúrate de que los contenedores de Docker estén corriendo con el comando docker-compose -f docker-compose.yml up -d.

Acceder a Jupyter Notebook:

Abre tu navegador web y navega a http://localhost:8888.

No se requiere contraseña, ya que el contenedor está configurado para no pedirla.

Ejecutar el análisis:

Dentro del entorno de Jupyter, navega a la carpeta notebooks.

Abre el archivo analisis_gold_data.ipynb.

Ejecuta las celdas del notebook para cargar los datos de gold_data y ver las visualizaciones.

Este notebook te permite interactuar con los datos procesados y sirve como punto de partida para análisis más avanzados o la creación de reportes.


