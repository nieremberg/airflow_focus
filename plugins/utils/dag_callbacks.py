"""
Callbacks para DAGs do FinOps Pipeline.

Este módulo fornece callbacks padronizados para:
- Início/fim de DAG
- Sucesso/falha de tasks
- Retry de tasks

Os callbacks utilizam logging estruturado para facilitar debugging e monitoramento.
"""

import time
from typing import Any, Dict
from utils.logging_config import get_logger


def on_dag_start(context: Dict[str, Any]) -> None:
    """
    Callback executado quando uma DAG inicia.
    
    Args:
        context: Contexto do Airflow contendo informações da execução.
    """
    dag_id = context.get('dag').dag_id
    execution_date = context.get('execution_date')
    run_id = context.get('run_id')
    
    logger = get_logger(__name__, dag_id=dag_id)
    
    logger.log_separator(f"DAG INICIADA: {dag_id}")
    logger.info(
        f"Execução iniciada",
        phase='start',
        dag_id=dag_id,
        execution_date=str(execution_date),
        run_id=run_id
    )


def on_dag_success(context: Dict[str, Any]) -> None:
    """
    Callback executado quando uma DAG completa com sucesso.
    
    Args:
        context: Contexto do Airflow contendo informações da execução.
    """
    dag_id = context.get('dag').dag_id
    execution_date = context.get('execution_date')
    
    logger = get_logger(__name__, dag_id=dag_id)
    
    logger.info(
        f"DAG completada com sucesso",
        phase='success',
        dag_id=dag_id,
        execution_date=str(execution_date)
    )
    logger.log_separator(f"DAG CONCLUÍDA: {dag_id}")


def on_dag_failure(context: Dict[str, Any]) -> None:
    """
    Callback executado quando uma DAG falha.
    
    Args:
        context: Contexto do Airflow contendo informações da execução.
    """
    dag_id = context.get('dag').dag_id
    execution_date = context.get('execution_date')
    task_instance = context.get('task_instance')
    exception = context.get('exception')
    
    logger = get_logger(__name__, dag_id=dag_id)
    
    logger.error(
        f"DAG falhou: {str(exception)}",
        phase='error',
        dag_id=dag_id,
        execution_date=str(execution_date),
        failed_task=task_instance.task_id if task_instance else None,
        error_type=type(exception).__name__ if exception else None
    )
    logger.log_separator(f"DAG FALHOU: {dag_id}")


def on_task_success(context: Dict[str, Any]) -> None:
    """
    Callback executado quando uma task completa com sucesso.
    
    Args:
        context: Contexto do Airflow contendo informações da execução.
    """
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    
    logger = get_logger(__name__, dag_id=dag_id, task_id=task_id)
    
    # Calcular duração
    duration = None
    if task_instance.start_date and task_instance.end_date:
        duration = (task_instance.end_date - task_instance.start_date).total_seconds() * 1000
    
    logger.info(
        f"Task '{task_id}' completada com sucesso",
        phase='success',
        dag_id=dag_id,
        task_id=task_id,
        execution_date=str(execution_date),
        duration_ms=round(duration, 2) if duration else None
    )


def on_task_failure(context: Dict[str, Any]) -> None:
    """
    Callback executado quando uma task falha.
    
    Args:
        context: Contexto do Airflow contendo informações da execução.
    """
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    
    logger = get_logger(__name__, dag_id=dag_id, task_id=task_id)
    
    logger.error(
        f"Task '{task_id}' falhou: {str(exception)}",
        exc_info=True,
        phase='error',
        dag_id=dag_id,
        task_id=task_id,
        execution_date=str(execution_date),
        error_type=type(exception).__name__ if exception else None,
        try_number=task_instance.try_number
    )


def on_task_retry(context: Dict[str, Any]) -> None:
    """
    Callback executado quando uma task vai ser re-tentada.
    
    Args:
        context: Contexto do Airflow contendo informações da execução.
    """
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    exception = context.get('exception')
    
    logger = get_logger(__name__, dag_id=dag_id, task_id=task_id)
    
    logger.warning(
        f"Task '{task_id}' será re-tentada (tentativa {task_instance.try_number})",
        phase='progress',
        dag_id=dag_id,
        task_id=task_id,
        try_number=task_instance.try_number,
        max_tries=task_instance.max_tries,
        error_type=type(exception).__name__ if exception else None
    )


def create_default_args_with_callbacks(
    owner: str = "airflow",
    retries: int = 5,
    retry_delay_minutes: int = 2,
    **kwargs
) -> Dict[str, Any]:
    """
    Cria default_args padrão com callbacks de logging.
    
    Args:
        owner: Dono da DAG.
        retries: Número de tentativas em caso de falha.
        retry_delay_minutes: Delay entre tentativas em minutos.
        **kwargs: Argumentos adicionais.
    
    Returns:
        Dicionário com default_args configurados.
    """
    from datetime import timedelta
    
    return {
        "owner": owner,
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": retries,
        "retry_delay": timedelta(minutes=retry_delay_minutes),
        "on_success_callback": on_task_success,
        "on_failure_callback": on_task_failure,
        "on_retry_callback": on_task_retry,
        **kwargs
    }
