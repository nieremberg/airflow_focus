"""
Módulo de configuração de logging estruturado para as DAGs do FinOps Pipeline.

Este módulo fornece um logger estruturado e verboso que:
- Usa stdout para logs normais (evita falsos positivos de ERROR no Airflow)
- Fornece contexto rico de execução (DAG, task, provider, etc.)
- Usa emojis mínimos para indicar início, fim, atenção e erro
- Registra métricas de performance automaticamente
"""

import sys
import logging
import time
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, Optional


class StructuredFormatter(logging.Formatter):
    """
    Formatter customizado que produz logs estruturados com
    informações visuais para facilitar a leitura.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Formata o registro de log em formato estruturado."""
        
        # Timestamp formatado
        timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
        
        # Determinar emoji baseado no nível e fase
        emoji = ""
        phase = getattr(record, 'phase', None)
        
        if record.levelname == 'ERROR' or record.levelname == 'CRITICAL':
            emoji = "❌ "
        elif record.levelname == 'WARNING':
            emoji = "⚠️ "
        elif phase == 'start':
            emoji = "🚀 "
        elif phase == 'end':
            emoji = "🏁 "
        elif record.levelname == 'INFO':
            emoji = "✅ "
        
        # Adicionar contexto extra se disponível
        extra_data = {}
        for key in ['dag_id', 'task_id', 'provider', 'phase', 'duration_ms', 
                    'records_count', 'files_count', 'error_type', 'bucket', 
                    'prefix', 'temp_dir', 'file_extension']:
            if hasattr(record, key) and getattr(record, key) is not None:
                extra_data[key] = getattr(record, key)
        
        # Construir mensagem base
        message = f"{emoji}[{timestamp}] [{record.levelname}] {record.getMessage()}"
        
        # Adicionar contexto se houver dados extras relevantes
        if extra_data:
            # Filtrar apenas dados importantes para exibição
            display_keys = ['provider', 'records_count', 'files_count', 'duration_ms']
            display_data = {k: v for k, v in extra_data.items() if k in display_keys}
            if display_data:
                context_str = " | ".join([f"{k}={v}" for k, v in display_data.items()])
                message = f"{message} [{context_str}]"
        
        # Adicionar traceback se houver exceção
        if record.exc_info:
            message = f"{message}\n{self.formatException(record.exc_info)}"
        
        return message


class StdoutHandler(logging.StreamHandler):
    """
    Handler que envia logs para stdout (não stderr).
    Isso evita que o Airflow marque logs INFO como ERROR.
    """
    def __init__(self):
        super().__init__(sys.stdout)


class FinOpsLogger:
    """
    Logger estruturado para operações do FinOps Pipeline.
    
    Fornece métodos convenientes para logging com contexto rico
    e medição automática de performance.
    """
    
    def __init__(self, name: str, dag_id: Optional[str] = None, task_id: Optional[str] = None):
        """
        Inicializa o logger estruturado.
        
        Args:
            name: Nome do logger (geralmente __name__ do módulo)
            dag_id: ID da DAG atual (opcional)
            task_id: ID da task atual (opcional)
        """
        self.logger = logging.getLogger(f"finops.{name}")
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False  # Evita duplicação de logs
        
        # Remover handlers existentes para evitar duplicação
        self.logger.handlers.clear()
        
        # Usar stdout em vez de stderr
        handler = StdoutHandler()
        handler.setFormatter(StructuredFormatter())
        handler.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)
        
        self.dag_id = dag_id
        self.task_id = task_id
        self._context: Dict[str, Any] = {}
    
    def set_context(self, **kwargs) -> None:
        """Define contexto adicional para todos os logs subsequentes."""
        self._context.update(kwargs)
    
    def clear_context(self) -> None:
        """Limpa o contexto adicional."""
        self._context.clear()
    
    def _add_context(self, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Combina contexto base com contexto adicional."""
        context = {
            'dag_id': self.dag_id,
            'task_id': self.task_id,
            **self._context
        }
        if extra:
            context.update(extra)
        return {k: v for k, v in context.items() if v is not None}
    
    def debug(self, message: str, **kwargs) -> None:
        """Log de nível DEBUG."""
        self.logger.debug(message, extra=self._add_context(kwargs))
    
    def info(self, message: str, **kwargs) -> None:
        """Log de nível INFO."""
        self.logger.info(message, extra=self._add_context(kwargs))
    
    def warning(self, message: str, **kwargs) -> None:
        """Log de nível WARNING."""
        self.logger.warning(message, extra=self._add_context(kwargs))
    
    def error(self, message: str, exc_info: bool = False, **kwargs) -> None:
        """Log de nível ERROR."""
        self.logger.error(message, exc_info=exc_info, extra=self._add_context(kwargs))
    
    def critical(self, message: str, exc_info: bool = False, **kwargs) -> None:
        """Log de nível CRITICAL."""
        self.logger.critical(message, exc_info=exc_info, extra=self._add_context(kwargs))
    
    def log_separator(self, title: str = "", char: str = "═", length: int = 70) -> None:
        """Imprime um separador visual no log."""
        if title:
            padding = (length - len(title) - 2) // 2
            separator = f"{char * padding} {title} {char * padding}"
        else:
            separator = char * length
        print(separator, flush=True)
    
    def log_task_start(self, task_name: str, **kwargs) -> None:
        """Log de início de uma task."""
        self.log_separator(f"INÍCIO: {task_name}")
        self.info(f"Iniciando task: {task_name}", phase='start', **kwargs)
    
    def log_task_end(self, task_name: str, duration_ms: Optional[float] = None, **kwargs) -> None:
        """Log de fim de uma task."""
        msg = f"Task concluída: {task_name}"
        if duration_ms:
            msg += f" ({duration_ms:.2f}ms)"
        self.info(msg, phase='end', duration_ms=duration_ms, **kwargs)
        self.log_separator(f"FIM: {task_name}")
    
    def log_step(self, step_name: str, step_number: int, total_steps: int, **kwargs) -> None:
        """Log de um passo intermediário."""
        self.info(f"[{step_number}/{total_steps}] {step_name}", **kwargs)
    
    def log_metrics(self, metrics: Dict[str, Any], **kwargs) -> None:
        """Log de métricas de execução."""
        metrics_str = " | ".join([f"{k}: {v}" for k, v in metrics.items()])
        self.info(f"Métricas: {metrics_str}", **kwargs)
    
    @contextmanager
    def log_operation(self, operation_name: str, **kwargs):
        """
        Context manager para logging automático de operações com timing.
        """
        start_time = time.time()
        operation_data = {'operation': operation_name, **kwargs}
        
        self.info(f"Iniciando: {operation_name}", phase='start', **operation_data)
        
        result = {}
        try:
            yield result
            duration_ms = (time.time() - start_time) * 1000
            result['duration_ms'] = duration_ms
            self.info(
                f"Concluído: {operation_name}", 
                phase='end', 
                duration_ms=duration_ms,
                **{**operation_data, **result}
            )
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            self.error(
                f"Erro em {operation_name}: {str(e)}",
                exc_info=True,
                phase='error',
                duration_ms=duration_ms,
                error_type=type(e).__name__,
                **operation_data
            )
            raise


def get_logger(name: str, dag_id: Optional[str] = None, task_id: Optional[str] = None) -> FinOpsLogger:
    """
    Factory function para criar um logger estruturado.
    
    Args:
        name: Nome do logger (geralmente __name__)
        dag_id: ID da DAG (opcional)
        task_id: ID da task (opcional)
    
    Returns:
        FinOpsLogger configurado
    """
    return FinOpsLogger(name, dag_id, task_id)


def log_execution(dag_id: Optional[str] = None, task_id: Optional[str] = None):
    """
    Decorator para adicionar logging automático a funções de task.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__, dag_id=dag_id, task_id=task_id)
            
            ti = kwargs.get('ti') or kwargs.get('task_instance')
            if ti:
                logger.dag_id = logger.dag_id or ti.dag_id
                logger.task_id = logger.task_id or ti.task_id
            
            logger.log_task_start(func.__name__)
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                duration_ms = (time.time() - start_time) * 1000
                logger.log_task_end(func.__name__, duration_ms=duration_ms)
                return result
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                logger.error(
                    f"Erro na execução de {func.__name__}: {str(e)}",
                    exc_info=True,
                    phase='error',
                    duration_ms=duration_ms,
                    error_type=type(e).__name__
                )
                raise
        
        return wrapper
    return decorator
