"""
Este módulo fornece uma implementação da interface StorageInterface para
interagir com o armazenamento GCP Cloud Storage via API S3-compatible (HMAC keys).
"""

import os
import boto3
from typing import List
from airflow.sdk import Variable
from botocore.exceptions import ClientError
from interfaces.storage_interface import StorageInterface


class GCPStorage(StorageInterface):
    def __init__(self, bucket: str):
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=Variable.get("GCP_ACCESS_KEY"),
            aws_secret_access_key=Variable.get("GCP_SECRET_KEY"),
            endpoint_url="https://storage.googleapis.com",
        )
        self.bucket = bucket

    def list_objects(self, prefix: str = "") -> List[str]:
        """
        Lista todos os objetos no bucket GCP com o prefixo especificado.

        Args:
            prefix: Prefixo para filtrar objetos

        Returns:
            Lista de chaves de objetos

        Raises:
            ClientError: Em caso de erro na API
        """
        keys = []
        continuation_token = None

        while True:
            list_kwargs = {
                "Bucket": self.bucket,
                "Prefix": prefix,
            }
            if continuation_token:
                list_kwargs["ContinuationToken"] = continuation_token

            response = self.s3.list_objects_v2(**list_kwargs)
            keys.extend([obj["Key"] for obj in response.get("Contents", [])])

            if not response.get("IsTruncated"):
                break

            continuation_token = response.get("NextContinuationToken")

        return keys

    def download_object(self, key, destination):
        """
        Faz o download de um objeto do GCP Cloud Storage.

        Args:
            key: Chave do objeto no bucket
            destination: Caminho local para salvar o arquivo

        Raises:
            ClientError: Em caso de erro na API
        """
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        try:
            self.s3.download_file(self.bucket, key, destination)
        except ClientError as e:
            raise RuntimeError(f"Erro ao baixar {key}: {e}") from e

    def upload_object(self, source, key):
        """
        Faz o upload de um arquivo para o GCP Cloud Storage.

        Args:
            source: Caminho local do arquivo
            key: Chave de destino no bucket

        Raises:
            NotImplementedError: Método não implementado
        """
        raise NotImplementedError("O método 'upload_object' não implementado.")
