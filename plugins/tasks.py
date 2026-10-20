"""
Módulo de tarefas para manipulação de arquivos, gravação no banco de dados e 
integração com provedores de nuvem.

As funções são projetadas para trabalhar com diferentes provedores de 
armazenamento e utilizam fábricas de carregadores e armazenamento para abstrair 
a lógica específica do provedor.

Todas as funções utilizam logging estruturado para facilitar debugging e
monitoramento das operações.
"""

import os
import json
import time
from typing import Optional, List, Union

from loaders.loader_factory import get_loader
from utils.database import get_processed_files
from storages.storage_factory import get_storage
from utils.logging_config import get_logger


def list_processed_keys_task(provider: str) -> str:
    """
    Lista as chaves processadas para um determinado provedor.
    
    Esta função recupera a lista de arquivos processados associados ao
    provedor especificado no banco de dados.
    
    Args:
        provider: Sigla do provedor para o qual listar as chaves processadas.
        
    Returns:
        Uma string codificada em JSON contendo a lista de chaves processadas.
    """
    logger = get_logger(__name__, task_id='list_processed_keys')
    logger.set_context(provider=provider)
    
    logger.log_task_start('list_processed_keys')
    start_time = time.time()
    
    try:
        logger.info(
            f"Consultando banco de dados para chaves processadas",
            phase='database',
            provider=provider
        )
        
        list_processed = get_processed_files(provider)
        
        if not list_processed:
            logger.warning(
                f"Nenhuma chave processada encontrada para o provedor '{provider}'",
                provider=provider,
                records_count=0
            )
        else:
            logger.info(
                f"Encontradas {len(list_processed)} chaves processadas",
                provider=provider,
                records_count=len(list_processed)
            )
            # Log das primeiras 5 chaves como amostra
            sample_keys = list_processed[:5]
            logger.debug(
                f"Amostra das chaves (primeiras 5): {sample_keys}",
                provider=provider
            )
        
        result = json.dumps(list_processed)
        
        duration_ms = (time.time() - start_time) * 1000
        logger.log_metrics({
            'total_keys': len(list_processed),
            'duration_ms': round(duration_ms, 2),
            'provider': provider
        })
        logger.log_task_end('list_processed_keys', duration_ms=duration_ms)
        
        return result
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        logger.error(
            f"Erro ao listar chaves processadas para '{provider}': {str(e)}",
            exc_info=True,
            phase='error',
            provider=provider,
            duration_ms=duration_ms,
            error_type=type(e).__name__
        )
        raise


def download_new_objects_task(
    provider: str,
    bucket: str,
    prefix: str,
    temp_dir: str,
    list_processed_files: Union[str, List[str]]
) -> None:
    """
    Faz o download de novos objetos para um diretório temporário.
    
    Este método verifica os objetos disponíveis no bucket especificado e os 
    compara com uma lista de objetos já existentes.

    Args:
        provider: Sigla do provedor de armazenamento (ex.: aws, hcso, oci).
        bucket: O nome do bucket de onde os objetos serão baixados.
        prefix: O prefixo usado para filtrar os objetos no bucket.
        temp_dir: O diretório local onde os novos objetos serão armazenados.
        list_processed_files: Lista de objetos já existentes ou uma string JSON.
    """
    logger = get_logger(__name__, task_id='download_new_objects')
    logger.set_context(provider=provider, bucket=bucket, prefix=prefix, temp_dir=temp_dir)
    
    logger.log_task_start('download_new_objects')
    start_time = time.time()
    
    new_files = 0
    max_downloads = 100
    
    try:
        # Step 1: Inicializar storage
        logger.log_step("Inicializando conexão com storage", 1, 5, provider=provider)
        storage = get_storage(provider, bucket=bucket)
        logger.info(
            f"Storage inicializado com sucesso",
            phase='progress',
            provider=provider,
            bucket=bucket
        )
        
        # Step 2: Criar diretório temporário
        logger.log_step("Criando diretório temporário", 2, 5, temp_dir=temp_dir)
        os.makedirs(temp_dir, exist_ok=True)
        logger.debug(f"Diretório temporário criado/verificado: {temp_dir}")
        
        # Step 3: Parsear lista de arquivos processados
        logger.log_step("Processando lista de arquivos já existentes", 3, 5)
        if isinstance(list_processed_files, str):
            list_processed_files = json.loads(list_processed_files)
        processed_count = len(list_processed_files)
        logger.info(
            f"Total de arquivos já processados: {processed_count}",
            records_count=processed_count
        )
        
        # Step 4: Listar objetos no bucket
        logger.log_step("Listando objetos no bucket remoto", 4, 5, bucket=bucket, prefix=prefix)
        all_keys = storage.list_objects(prefix)
        total_remote = len(all_keys)
        logger.info(
            f"Total de objetos no bucket: {total_remote}",
            records_count=total_remote,
            bucket=bucket,
            prefix=prefix
        )
        
        # Identificar novos objetos
        new_keys = [k for k in all_keys if k not in list_processed_files]
        logger.info(
            f"Novos objetos para download: {len(new_keys)}",
            records_count=len(new_keys),
            provider=provider
        )
        
        if new_keys:
            logger.debug(f"Amostra dos novos objetos (primeiros 5): {new_keys[:5]}")
        
        # Step 5: Download dos novos objetos
        logger.log_step("Executando downloads", 5, 5)
        download_errors = []
        
        for idx, key in enumerate(new_keys, 1):
            if new_files >= max_downloads:
                logger.warning(
                    f"Limite de downloads atingido ({max_downloads}). "
                    f"Restam {len(new_keys) - new_files} arquivos para próxima execução.",
                    files_count=new_files,
                    remaining=len(new_keys) - new_files
                )
                break
            
            local_path = os.path.join(temp_dir, key)
            
            try:
                download_start = time.time()
                storage.download_object(key, local_path)
                download_duration = (time.time() - download_start) * 1000
                
                new_files += 1
                
                # Log a cada 10 arquivos ou no primeiro/último
                if new_files == 1 or new_files % 10 == 0 or new_files == min(len(new_keys), max_downloads):
                    logger.info(
                        f"Download {new_files}/{min(len(new_keys), max_downloads)}: {key}",
                        phase='download',
                        files_count=new_files,
                        duration_ms=round(download_duration, 2)
                    )
                else:
                    logger.debug(
                        f"Download {new_files}: {key}",
                        phase='download',
                        duration_ms=round(download_duration, 2)
                    )
                    
            except Exception as e:
                download_errors.append({'key': key, 'error': str(e)})
                logger.error(
                    f"Erro ao baixar '{key}': {str(e)}",
                    phase='error',
                    error_type=type(e).__name__
                )
        
        # Métricas finais
        duration_ms = (time.time() - start_time) * 1000
        logger.log_metrics({
            'provider': provider,
            'bucket': bucket,
            'total_remote_objects': total_remote,
            'already_processed': processed_count,
            'new_objects_found': len(new_keys),
            'downloaded': new_files,
            'errors': len(download_errors),
            'duration_ms': round(duration_ms, 2)
        })
        
        if download_errors:
            logger.warning(
                f"{len(download_errors)} erro(s) durante o download",
                files_count=len(download_errors)
            )
        
        logger.log_task_end('download_new_objects', duration_ms=duration_ms)
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        logger.error(
            f"Erro crítico no download de objetos: {str(e)}",
            exc_info=True,
            phase='error',
            provider=provider,
            bucket=bucket,
            duration_ms=duration_ms,
            error_type=type(e).__name__
        )
        raise


def load_to_db_task(
    provider: str,
    temp_dir: str,
    file_extension: str,
    paths: Optional[Union[str, List[str]]] = None
) -> None:
    """
    Carrega dados para o banco de dados usando um carregador específico do provedor.

    Args:
        provider: Sigla do provedor que será usado para carregar os dados.
        temp_dir: O diretório temporário onde os dados estão armazenados.
        file_extension: A extensão do arquivo que será carregado.
        paths: Uma lista de caminhos para os arquivos que serão carregados.
    """
    logger = get_logger(__name__, task_id='load_to_db')
    logger.set_context(provider=provider, temp_dir=temp_dir, file_extension=file_extension)
    
    logger.log_task_start('load_to_db')
    start_time = time.time()
    
    try:
        # Contar arquivos para processar
        if paths:
            if isinstance(paths, str):
                try:
                    paths = json.loads(paths)
                except json.JSONDecodeError:
                    paths = [paths]
            files_to_process = len(paths) if isinstance(paths, list) else 1
        else:
            files_to_process = sum(
                1 for _, _, files in os.walk(temp_dir)
                for f in files if f.endswith(file_extension)
            )
        
        logger.info(
            f"Arquivos identificados para carga: {files_to_process}",
            phase='database',
            files_count=files_to_process,
            file_extension=file_extension
        )
        
        # Inicializar e executar loader
        logger.info(f"Inicializando loader para '{provider}'", phase='process')
        loader = get_loader(provider, temp_dir)
        
        logger.info(
            f"Executando carga de dados no banco",
            phase='database',
            provider=provider
        )
        loader.load(file_extension=file_extension, paths=paths)
        
        duration_ms = (time.time() - start_time) * 1000
        logger.log_metrics({
            'provider': provider,
            'files_processed': files_to_process,
            'file_extension': file_extension,
            'duration_ms': round(duration_ms, 2)
        })
        
        logger.log_task_end('load_to_db', duration_ms=duration_ms)
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        logger.error(
            f"Erro ao carregar dados no banco: {str(e)}",
            exc_info=True,
            phase='error',
            provider=provider,
            duration_ms=duration_ms,
            error_type=type(e).__name__
        )
        raise


def upload_to_dell_ecs_task(
    provider: str,
    bucket: str,
    prefix: str,
    temp_dir: str,
    file_extension: str
) -> None:
    """
    Faz o upload de arquivos para um bucket Dell ECS.

    Args:
        provider: O provedor de armazenamento a ser utilizado.
        bucket: O nome do bucket Dell ECS onde os arquivos serão armazenados.
        prefix: O prefixo a ser adicionado às chaves dos arquivos no bucket.
        temp_dir: O diretório temporário onde os arquivos estão localizados.
        file_extension: A extensão dos arquivos que devem ser enviados.
    """
    logger = get_logger(__name__, task_id='upload_to_dell_ecs')
    logger.set_context(
        provider=provider, 
        bucket=bucket, 
        prefix=prefix, 
        temp_dir=temp_dir,
        file_extension=file_extension
    )
    
    logger.log_task_start('upload_to_dell_ecs')
    start_time = time.time()
    
    new_files = 0
    upload_errors = []
    
    try:
        # Contar arquivos para upload
        total_files = sum(
            1 for _, _, files in os.walk(temp_dir)
            for f in files if f.lower().endswith(file_extension)
        )
        
        logger.info(
            f"Arquivos identificados para upload: {total_files}",
            files_count=total_files,
            file_extension=file_extension
        )
        
        if total_files == 0:
            logger.warning(
                f"Nenhum arquivo com extensão '{file_extension}' encontrado em '{temp_dir}'",
                temp_dir=temp_dir,
                file_extension=file_extension
            )
            duration_ms = (time.time() - start_time) * 1000
            logger.log_task_end('upload_to_dell_ecs', duration_ms=duration_ms)
            return
        
        # Inicializar storage
        logger.info(f"Inicializando conexão com Dell ECS", phase='process')
        storage = get_storage(provider, bucket=bucket)
        
        # Upload dos arquivos
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                if file.lower().endswith(file_extension):
                    path = os.path.join(root, file)
                    key = path.split(temp_dir, 1)[-1].lstrip("/")
                    full_key = f"{prefix}{key}"
                    
                    try:
                        upload_start = time.time()
                        storage.upload_object(path, full_key)
                        upload_duration = (time.time() - upload_start) * 1000
                        
                        new_files += 1
                        
                        # Log a cada 10 arquivos ou no primeiro/último
                        if new_files == 1 or new_files % 10 == 0 or new_files == total_files:
                            logger.info(
                                f"Upload {new_files}/{total_files}: {key}",
                                phase='upload',
                                files_count=new_files,
                                duration_ms=round(upload_duration, 2)
                            )
                        else:
                            logger.debug(
                                f"Upload {new_files}: {key}",
                                phase='upload',
                                duration_ms=round(upload_duration, 2)
                            )
                            
                    except Exception as e:
                        upload_errors.append({'key': key, 'error': str(e)})
                        logger.error(
                            f"Erro ao fazer upload de '{key}': {str(e)}",
                            phase='error',
                            error_type=type(e).__name__
                        )
        
        duration_ms = (time.time() - start_time) * 1000
        logger.log_metrics({
            'provider': provider,
            'bucket': bucket,
            'prefix': prefix,
            'total_files': total_files,
            'uploaded': new_files,
            'errors': len(upload_errors),
            'duration_ms': round(duration_ms, 2)
        })
        
        if upload_errors:
            logger.warning(
                f"{len(upload_errors)} erro(s) durante o upload",
                files_count=len(upload_errors)
            )
        
        logger.log_task_end('upload_to_dell_ecs', duration_ms=duration_ms)
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        logger.error(
            f"Erro crítico no upload para Dell ECS: {str(e)}",
            exc_info=True,
            phase='error',
            provider=provider,
            bucket=bucket,
            duration_ms=duration_ms,
            error_type=type(e).__name__
        )
        raise

