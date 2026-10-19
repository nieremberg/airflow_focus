"""
DAG para carregar dados do Huawei Cloud (Focus) para o Postgres e sincronizar
novos objetos para o Dell ECS.

Pipeline ETL que:
1. Lista arquivos já processados no banco
2. Baixa novos objetos do bucket HPC Focus
3. Descompacta arquivos .zip
4. Carrega dados no PostgreSQL
5. Sincroniza para Dell ECS
6. Limpa diretório temporário
"""

from datetime import datetime

from tasks import (
    list_processed_keys_task,
    download_new_objects_task,
    upload_to_dell_ecs_task,
    load_to_db_task,
)

from utils.files import unpack_zip_files, clean_temp_dir
from utils.dag_callbacks import create_default_args_with_callbacks, on_dag_success, on_dag_failure

from airflow import DAG
from airflow.sdk import Variable
from airflow.operators.python import PythonOperator

default_args = create_default_args_with_callbacks(
    owner="airflow",
    retries=5,
    retry_delay_minutes=2
)

with DAG(
    dag_id="hpc_focus_pipeline",
    description="Carrega dados da Huawei (Focus) para o banco de dados e sincroniza os novos objetos para o Dell ECS",
    tags=["huawei", "focus", "etl"],
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=True,
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
) as dag:

    list_processed_keys = PythonOperator(
        task_id="list_processed_keys",
        python_callable=list_processed_keys_task,
        op_kwargs={
            "provider": "hpc_focus",
        },
    )

    download_new_objects = PythonOperator(
        task_id="download_new_objects",
        python_callable=download_new_objects_task,
        op_kwargs={
            "provider": "hpc_focus",
            "bucket": Variable.get("HPC_BUCKET"),
            "prefix": Variable.get("HPC_FOCUS_PREFIX"),
            "temp_dir": "/tmp/hpc_downloads",
            "list_processed_files": "{{ task_instance.xcom_pull(task_ids='list_processed_keys') }}",
        },
    )

    unpack_files = PythonOperator(
        task_id="unpack_files",
        python_callable=unpack_zip_files,
        op_kwargs={
            "temp_dir": "/tmp/hpc_downloads",
        },
    )

    load_to_db = PythonOperator(
        task_id="load_to_db",
        python_callable=load_to_db_task,
        op_kwargs={
            "provider": "hpc_focus", 
            "temp_dir": "/tmp/hpc_downloads",
            "file_extension": ".zip",
            "paths": "{{ task_instance.xcom_pull(task_ids='unpack_files') }}",
        },
    )

    upload_to_dell_ecs = PythonOperator(
        task_id="upload_to_dell_ecs",
        python_callable=upload_to_dell_ecs_task,
        op_kwargs={
            "provider": "ecs",
            "bucket": Variable.get("ECS_BUCKET"),
            "prefix": Variable.get("ECS_HPC_FOCUS_PREFIX"),
            "temp_dir": "/tmp/hpc_downloads",
            "file_extension": ".zip",
        },
    )

    clean_task = PythonOperator(
        task_id="clean_temp_dir",
        python_callable=clean_temp_dir,
        op_kwargs={
            "temp_dir": "/tmp/hpc_downloads",
        },
    )

    (
        list_processed_keys
        >> download_new_objects
        >> unpack_files
        >> load_to_db
        >> upload_to_dell_ecs
        >> clean_task
    )
