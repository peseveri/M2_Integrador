from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

# Reemplaza con tu nombre de usuario de Docker Hub
DOCKER_USER = "pedroseveri"
# La ruta absoluta de la carpeta del proyecto en tu máquina
HOST_PROJECT_PATH = "/host_mnt/p/Accenture_Henry/M2_Integrador/Avance2_3_4"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_pipeline_docker',
    default_args=default_args,
    description='Un DAG para ejecutar el pipeline ETL con Docker.',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    tags=['etl', 'docker'],
) as dag:

    ingest_data_task = DockerOperator(
        task_id='ingest_data',
        image=f"{DOCKER_USER}/etl-ingesta:v1",
        command="python ingesta_datos.py",
        mounts=[
            { 'type': 'bind', 'source': f"{HOST_PROJECT_PATH}/data_sources", 'target': '/app/data_sources' },
            { 'type': 'bind', 'source': f"{HOST_PROJECT_PATH}/raw_data", 'target': '/app/raw_data' }
        ],
        environment={
            "RAW_DATA_PATH": "/app/raw_data",
            "CSV_SOURCE_PATH": "/app/data_sources"
        },
        auto_remove=True
    )

    transform_data_task = DockerOperator(
        task_id='transform_data',
        image=f"{DOCKER_USER}/etl-transform:v1",
        command="spark-submit transform.py",
        mounts=[
            { 'type': 'bind', 'source': f"{HOST_PROJECT_PATH}/raw_data", 'target': '/app/raw_data' },
            { 'type': 'bind', 'source': f"{HOST_PROJECT_PATH}/silver_data", 'target': '/app/silver_data' },
            { 'type': 'bind', 'source': f"{HOST_PROJECT_PATH}/gold_data", 'target': '/app/gold_data' }
        ],
        environment={
            "RAW_DATA_PATH": "/app/raw_data",
            "SILVER_DATA_PATH": "/app/silver_data",
            "GOLD_DATA_PATH": "/app/gold_data"
        },
        auto_remove=True
    )

    # Define la dependencia del pipeline
    ingest_data_task >> transform_data_task
