"""
Módulo com funções utilitárias para manipulação de arquivos em DAGs do Airflow.

Inclui funções para:
- Limpeza de diretórios temporários
- Cálculo de hash MD5
- Ajuste de formatos de data
- Descompactação de arquivos (zip, gz, tar.gz)
- Ordenação de arquivos por timestamp

Todas as funções utilizam logging estruturado para facilitar debugging.
"""

import os
import re
import glob
import gzip
import json
import shutil
import tarfile
import zipfile
import hashlib
import time
import pandas as pd
from typing import List
from pathlib import Path
from typing import Optional
from datetime import datetime

from utils.logging_config import get_logger


def clean_temp_dir(temp_dir: Path) -> None:
    """
    Limpa os conteúdos do diretório temporário sem excluir o próprio diretório.

    Args:
        temp_dir: Caminho do diretório temporário
    """
    logger = get_logger(__name__, task_id='clean_temp_dir')
    logger.set_context(temp_dir=str(temp_dir))
    
    logger.log_task_start('clean_temp_dir')
    start_time = time.time()
    
    files_removed = 0
    dirs_removed = 0
    errors = []
    
    try:
        temp_dir = Path(temp_dir)
        
        if not temp_dir.exists():
            logger.warning(
                f"Diretório não existe: {temp_dir}",
                phase='clean'
            )
            return
        
        logger.info(
            f"Iniciando limpeza do diretório: {temp_dir}",
            phase='clean'
        )
        
        for item in temp_dir.iterdir():
            try:
                if item.is_file() or item.is_symlink():
                    item.unlink()
                    files_removed += 1
                    logger.debug(f"Arquivo removido: {item.name}")
                elif item.is_dir():
                    # Contar arquivos no diretório antes de remover
                    dir_files = sum(1 for _ in item.rglob('*') if _.is_file())
                    shutil.rmtree(item)
                    dirs_removed += 1
                    files_removed += dir_files
                    logger.debug(f"Diretório removido: {item.name} ({dir_files} arquivos)")
            except Exception as inner_e:
                errors.append({'item': str(item), 'error': str(inner_e)})
                logger.error(
                    f"Erro ao remover {item}: {inner_e}",
                    phase='error',
                    error_type=type(inner_e).__name__
                )
        
        duration_ms = (time.time() - start_time) * 1000
        logger.log_metrics({
            'temp_dir': str(temp_dir),
            'files_removed': files_removed,
            'dirs_removed': dirs_removed,
            'errors': len(errors),
            'duration_ms': round(duration_ms, 2)
        })
        
        logger.info(
            f"Limpeza concluída: {files_removed} arquivos, {dirs_removed} diretórios removidos",
            phase='success',
            files_count=files_removed
        )
        logger.log_task_end('clean_temp_dir', duration_ms=duration_ms)
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        logger.error(
            f"Erro crítico ao limpar diretório {temp_dir}: {e}",
            exc_info=True,
            phase='error',
            duration_ms=duration_ms,
            error_type=type(e).__name__
        )
        raise


def calculate_md5(file_path: str) -> str:
    """
    Calcula o hash MD5 de um arquivo.

    Args:
        file_path: Caminho do arquivo para calcular o hash MD5.

    Returns:
        O hash MD5 do arquivo.
    """
    logger = get_logger(__name__)
    
    logger.debug(f"Calculando MD5 para: {file_path}")
    start_time = time.time()
    
    with open(file_path, "rb") as f:
        md5_hash = hashlib.md5(f.read()).hexdigest()
    
    duration_ms = (time.time() - start_time) * 1000
    logger.debug(
        f"MD5 calculado: {md5_hash[:8]}... (duração: {duration_ms:.2f}ms)",
        duration_ms=duration_ms
    )
    
    return md5_hash


def ajustar_formatos_de_data(
    df: pd.DataFrame, 
    colunas_de_data: list, 
    formato: Optional[str] = None
) -> pd.DataFrame:
    """
    Ajusta os formatos das colunas de data em um DataFrame.

    Args:
        df: DataFrame contendo as colunas de data a serem ajustadas.
        colunas_de_data: Lista com os nomes das colunas de data.
        formato: Formato esperado da data para conversão (opcional).

    Returns:
        DataFrame com as colunas de data ajustadas.
    """
    logger = get_logger(__name__)
    
    logger.debug(
        f"Ajustando formato de {len(colunas_de_data)} colunas de data",
        records_count=len(df)
    )
    
    for coluna in colunas_de_data:
        df[coluna] = pd.to_datetime(df[coluna], format=formato, errors="coerce")
        df[coluna] = df[coluna].dt.strftime("%Y-%m-%d %H:%M:%S")
        logger.debug(f"Coluna '{coluna}' formatada")
    
    return df


def unpack_zip_files(temp_dir: Path, **kwargs) -> None:
    """
    Descompacta arquivos .zip presentes em determinado diretório.

    Esta função procura por todos os arquivos .zip dentro do diretório
    especificado e seus subdiretórios.

    Args:
        temp_dir: O caminho para o diretório temporário contendo arquivos .zip.
    """
    logger = get_logger(__name__, task_id='unpack_zip_files')
    logger.set_context(temp_dir=str(temp_dir))
    
    logger.log_task_start('unpack_zip_files')
    start_time = time.time()
    
    unpacked_count = 0
    errors = []
    
    try:
        temp_path = Path(temp_dir)
        
        logger.info(
            f"Procurando arquivos .zip em: {temp_path}",
            phase='process'
        )
        
        # Procura recursivamente por arquivos .zip
        zip_files = []
        if temp_path.exists():
            zip_files.extend(temp_path.rglob("*.zip"))
            zip_files.extend(temp_path.rglob("*.ZIP"))
        
        # Converte para set para evitar duplicados
        unique_zips = {str(p) for p in zip_files}
        
        logger.info(
            f"Arquivos .zip encontrados: {len(unique_zips)}",
            files_count=len(unique_zips)
        )
        
        if not unique_zips:
            logger.warning(
                f"Nenhum arquivo .zip encontrado em {temp_dir}",
                phase='process'
            )
            # Listar conteúdo para debug
            content_list = []
            for root, dirs, files in os.walk(temp_dir):
                for name in files:
                    content_list.append(os.path.join(root, name))
            logger.debug(
                f"Conteúdo do diretório ({len(content_list)} arquivos): {content_list[:10]}..."
            )
        
        for zip_path_str in unique_zips:
            zip_path = Path(zip_path_str)
            
            try:
                unpack_start = time.time()
                
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_name = zip_path.stem
                    extract_dir = zip_path.parent / zip_name
                    extract_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Contar arquivos no zip
                    file_count = len(zip_ref.namelist())
                    zip_ref.extractall(extract_dir)
                    
                    unpack_duration = (time.time() - unpack_start) * 1000
                    unpacked_count += 1
                    
                    logger.info(
                        f"Descompactado [{unpacked_count}/{len(unique_zips)}]: "
                        f"{zip_path.name} → {extract_dir} ({file_count} arquivos)",
                        phase='process',
                        files_count=file_count,
                        duration_ms=round(unpack_duration, 2)
                    )
                    
            except Exception as e:
                errors.append({'file': str(zip_path), 'error': str(e)})
                logger.error(
                    f"Erro ao descompactar {zip_path}: {e}",
                    phase='error',
                    error_type=type(e).__name__
                )
        
        duration_ms = (time.time() - start_time) * 1000
        logger.log_metrics({
            'temp_dir': str(temp_dir),
            'zip_files_found': len(unique_zips),
            'unpacked': unpacked_count,
            'errors': len(errors),
            'duration_ms': round(duration_ms, 2)
        })
        
        logger.log_task_end('unpack_zip_files', duration_ms=duration_ms)
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        logger.error(
            f"Erro crítico ao descompactar arquivos: {e}",
            exc_info=True,
            phase='error',
            duration_ms=duration_ms,
            error_type=type(e).__name__
        )
        raise


def unpack_gz_files(temp_dir: Path) -> None:
    """
    Descompacta arquivos .gz presentes em determinado diretório.

    Args:
        temp_dir: O caminho para o diretório temporário contendo arquivos .gz.
    """
    logger = get_logger(__name__, task_id='unpack_gz_files')
    logger.set_context(temp_dir=str(temp_dir))
    
    logger.log_task_start('unpack_gz_files')
    start_time = time.time()
    
    unpacked_count = 0
    errors = []
    
    try:
        pattern = os.path.join(temp_dir, "**", "*.gz")
        gz_files = glob.glob(pattern, recursive=True)
        
        logger.info(
            f"Arquivos .gz encontrados: {len(gz_files)}",
            files_count=len(gz_files)
        )
        
        for gz_path in gz_files:
            try:
                unpack_start = time.time()
                decompressed_path = os.path.splitext(gz_path)[0]
                
                with gzip.open(gz_path, "rb") as gz_file:
                    with open(decompressed_path, "wb") as decompressed_file:
                        shutil.copyfileobj(gz_file, decompressed_file)
                
                unpack_duration = (time.time() - unpack_start) * 1000
                unpacked_count += 1
                
                # Log a cada 10 arquivos
                if unpacked_count == 1 or unpacked_count % 10 == 0 or unpacked_count == len(gz_files):
                    logger.info(
                        f"Descompactado [{unpacked_count}/{len(gz_files)}]: "
                        f"{os.path.basename(gz_path)}",
                        phase='process',
                        duration_ms=round(unpack_duration, 2)
                    )
                else:
                    logger.debug(
                        f"Descompactado: {gz_path} → {decompressed_path}",
                        duration_ms=round(unpack_duration, 2)
                    )
                    
            except Exception as e:
                errors.append({'file': gz_path, 'error': str(e)})
                logger.error(
                    f"Erro ao descompactar {gz_path}: {e}",
                    phase='error',
                    error_type=type(e).__name__
                )
        
        duration_ms = (time.time() - start_time) * 1000
        logger.log_metrics({
            'temp_dir': str(temp_dir),
            'gz_files_found': len(gz_files),
            'unpacked': unpacked_count,
            'errors': len(errors),
            'duration_ms': round(duration_ms, 2)
        })
        
        logger.log_task_end('unpack_gz_files', duration_ms=duration_ms)
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        logger.error(
            f"Erro crítico ao descompactar arquivos .gz: {e}",
            exc_info=True,
            phase='error',
            duration_ms=duration_ms,
            error_type=type(e).__name__
        )
        raise


def decompress_zip(zip_file_path: Path, destination_folder: Path) -> None:
    """
    Descompacta um arquivo .zip no diretório de destino.
    
    Se o caminho do arquivo for muito longo, ele é renomeado antes da extração.

    Args:
        zip_file_path: Caminho para o arquivo .zip a ser extraído.
        destination_folder: Diretório onde os arquivos extraídos serão armazenados.
    """
    logger = get_logger(__name__)
    
    if len(str(zip_file_path)) > 100:
        renamed_zip = destination_folder / "a.zip"
        logger.debug(f"Renomeando arquivo com caminho longo: {zip_file_path} → {renamed_zip}")
        zip_file_path.rename(renamed_zip)
        zip_file_path = renamed_zip

    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(destination_folder)
        logger.debug(f"Extraído: {zip_file_path} → {destination_folder}")

        for file_name in zip_ref.namelist():
            decompressed_file_path = destination_folder / file_name
            if file_name.endswith(".zip"):
                logger.debug(f"Encontrado zip aninhado: {file_name}")
                decompress_zip(decompressed_file_path, destination_folder)


def decompress_sdr_files(temp_dir: Path, **kwargs) -> str:
    """
    Descompacta arquivos SDR do HCSO (.tar.gz) e extrai recursivamente os .zip.

    Args:
        temp_dir: Diretório temporário onde os arquivos .tar.gz estão armazenados.

    Returns:
        JSON string com lista de diretórios onde os arquivos foram extraídos.
    """
    logger = get_logger(__name__, task_id='decompress_sdr_files')
    logger.set_context(temp_dir=str(temp_dir))
    
    logger.log_task_start('decompress_sdr_files')
    start_time = time.time()
    
    temp_dir = Path(temp_dir)
    temp_dir.mkdir(exist_ok=True)
    
    paths = []
    tar_errors = []
    zip_errors = []
    
    try:
        # Encontrar arquivos .tar.gz
        tar_files = list(temp_dir.rglob("*.tar.gz"))
        logger.info(
            f"Arquivos .tar.gz encontrados: {len(tar_files)}",
            files_count=len(tar_files)
        )
        
        # Extrair arquivos .tar.gz
        for tar_file in tar_files:
            destino = tar_file.parent / (tar_file.stem)
            
            try:
                extract_start = time.time()
                
                with tarfile.open(tar_file, "r:gz") as tar:
                    tar.extractall(destino)
                
                extract_duration = (time.time() - extract_start) * 1000
                paths.append(str(destino))
                
                logger.info(
                    f"Extraído tar.gz: {tar_file.name} → {destino}",
                    phase='process',
                    duration_ms=round(extract_duration, 2)
                )
                
            except tarfile.ReadError:
                tar_errors.append({'file': str(tar_file), 'error': 'Não é um arquivo tar.gz válido'})
                logger.error(
                    f"Arquivo inválido: {tar_file.name} não é um tar.gz válido",
                    phase='error'
                )
            except Exception as e:
                tar_errors.append({'file': str(tar_file), 'error': str(e)})
                logger.error(
                    f"Erro ao processar {tar_file.name}: {e}",
                    phase='error',
                    error_type=type(e).__name__
                )
        
        # Extrair arquivos .zip dentro dos diretórios extraídos
        zip_count = 0
        for path in paths:
            path = Path(path)
            zip_files = list(path.rglob("*.zip"))
            
            logger.info(
                f"Arquivos .zip encontrados em {path.name}: {len(zip_files)}",
                files_count=len(zip_files)
            )
            
            for zip_file in zip_files:
                try:
                    decompress_zip(zip_file, zip_file.parent)
                    zip_count += 1
                    logger.debug(f"Extraído zip aninhado: {zip_file.name}")
                except Exception as e:
                    zip_errors.append({'file': str(zip_file), 'error': str(e)})
                    logger.error(
                        f"Erro ao descompactar {zip_file}: {e}",
                        phase='error',
                        error_type=type(e).__name__
                    )
        
        duration_ms = (time.time() - start_time) * 1000
        logger.log_metrics({
            'temp_dir': str(temp_dir),
            'tar_gz_files': len(tar_files),
            'tar_gz_extracted': len(paths),
            'zip_files_extracted': zip_count,
            'tar_errors': len(tar_errors),
            'zip_errors': len(zip_errors),
            'duration_ms': round(duration_ms, 2)
        })
        
        logger.log_task_end('decompress_sdr_files', duration_ms=duration_ms)
        
        return json.dumps(paths)
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        logger.error(
            f"Erro crítico ao descompactar arquivos SDR: {e}",
            exc_info=True,
            phase='error',
            duration_ms=duration_ms,
            error_type=type(e).__name__
        )
        raise


def aws_sort_files(paths: List[str]) -> List[str]:
    """
    Ordena uma lista de caminhos de arquivos CSV baseado no timestamp e número.

    Args:
        paths: Lista de caminhos de arquivos CSV.

    Returns:
        Lista de caminhos ordenados por timestamp e número do arquivo.
    """
    logger = get_logger(__name__)
    logger.debug(f"Ordenando {len(paths)} arquivos AWS por timestamp")
    
    def extrair_ordem(path: str) -> tuple:
        parts = path.split('/')
        timestamp = datetime.min
        numero_arquivo = 0
        
        for part in parts:
            if 'T' in part and 'Z' in part:
                try:
                    timestamp_str = part.split('Z')[0] + 'Z'
                    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                except ValueError:
                    pass
        
        if path.endswith('.csv'):
            nome_arquivo = path.split('/')[-1] 
            numero_str = nome_arquivo.split('-')[-1].split('.')[0]
            numero_arquivo = int(numero_str)
        
        return (timestamp, numero_arquivo)
    
    sorted_paths = sorted(paths, key=extrair_ordem)
    logger.debug(f"Arquivos ordenados com sucesso")
    
    return sorted_paths


def hpc_sort_files(paths: List[str]) -> List[str]:
    """
    Ordena uma lista de caminhos de arquivos HPC por competência e timestamp.

    Args:
        paths: Lista de caminhos de arquivos HPC.

    Returns:
        Lista de caminhos ordenados por competência e timestamp.
    """
    logger = get_logger(__name__)
    logger.debug(f"Ordenando {len(paths)} arquivos HPC por competência/timestamp")

    def extrair_ordem(path: str) -> tuple:
        competencia = 0
        timestamp = datetime.min

        competencia_match = re.search(r"/(\d{6})/", path)
        if competencia_match:
            competencia = int(competencia_match.group(1))

        timestamp_match = re.search(r"/(\d{8}T\d{6})Z?/", path)
        if timestamp_match:
            try:
                timestamp_str = timestamp_match.group(1)
                timestamp = datetime.strptime(timestamp_str, "%Y%m%dT%H%M%S")
            except ValueError:
                pass

        return (competencia, timestamp)

    sorted_paths = sorted(paths, key=extrair_ordem)
    logger.debug(f"Arquivos ordenados com sucesso")
    
    return sorted_paths
