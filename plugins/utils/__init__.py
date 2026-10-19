"""
Módulo de utilitários para o FinOps Pipeline.

Exporta funções e classes utilitárias para:
- Logging estruturado (logging_config)
- Callbacks de DAG (dag_callbacks)
- Manipulação de arquivos (files)
- Operações de banco de dados (database)
- Operações com DataFrames (dataframe)
"""

import os
import psycopg2

from utils.logging_config import get_logger, log_execution, FinOpsLogger
from utils.dag_callbacks import (
    on_dag_start,
    on_dag_success,
    on_dag_failure,
    on_task_success,
    on_task_failure,
    on_task_retry,
    create_default_args_with_callbacks,
)


def insert_dataframe(df, table):
    """
    Insere um DataFrame no banco de dados PostgreSQL.
    
    Args:
        df: DataFrame pandas com os dados a serem inseridos.
        table: Nome da tabela de destino.
    """
    logger = get_logger(__name__)
    
    logger.info(f"Inserindo {len(df)} registros na tabela '{table}'", phase='database')
    
    conn = psycopg2.connect(
        host=os.getenv('PGHOST', 'localhost'),
        database=os.getenv('PGDATABASE', 'airflow'),
        user=os.getenv('PGUSER', 'airflow'),
        password=os.getenv('PGPASSWORD', 'airflow')
    )
    cursor = conn.cursor()

    inserted = 0
    for _, row in df.iterrows():
        placeholders = ','.join(['%s'] * len(row))
        sql = f"INSERT INTO {table} ({','.join(df.columns)}) VALUES ({placeholders})"
        cursor.execute(sql, tuple(row))
        inserted += 1

    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info(f"Inserção concluída: {inserted} registros", phase='database', records_count=inserted)


__all__ = [
    # Logging
    'get_logger',
    'log_execution',
    'FinOpsLogger',
    # Callbacks
    'on_dag_start',
    'on_dag_success',
    'on_dag_failure',
    'on_task_success',
    'on_task_failure',
    'on_task_retry',
    'create_default_args_with_callbacks',
    # Database
    'insert_dataframe',
]
