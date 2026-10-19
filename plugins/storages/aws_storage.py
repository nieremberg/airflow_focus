"""
Este módulo fornece uma implementação da interface StorageInterface para
interagir com o armazenamento AWS S3.
"""

import os
import boto3
from typing import List
from airflow.sdk import Variable
from botocore.exceptions import ClientError
from interfaces.storage_interface import StorageInterface


class AWSStorage(StorageInterface):
    def __init__(self, bucket: str):
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        )
        self.bucket = bucket

    def list_objects(self, prefix: str = "") -> List[str]:
        """
        Lista todos os objetos no bucket S3 com o prefixo especificado.

        Args:
            prefix: Prefixo para filtrar objetos

        Returns:
            Lista de chaves de objetos

        Raises:
            ClientError: Em caso de erro na API da AWS
        """
        # response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        # return [obj["Key"] for obj in response.get("Contents", [])]
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
        Faz o download de um objeto do S3.

        Args:
            key: Chave do objeto no S3
            destination: Caminho local para salvar o arquivo

        Raises:
            ClientError: Em caso de erro na API da AWS
            ValueError: Se a chave não contiver um nome de arquivo
            FileNotFoundError: Se o diretório de destino não puder ser criado
        """
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        try:
            self.s3.download_file(self.bucket, key, destination)
        except ClientError as e:
            raise RuntimeError(f"Erro ao baixar {key}: {e}") from e

    def upload_object(self, source, key):
        """
        Faz o upload de um arquivo para o S3.

        Args:
            source: Caminho local do arquivo
            key: Chave de destino no S3
            extra_args: Argumentos extras para o upload (e.g., Metadata, ContentType)

        Raises:
            FileNotFoundError: Se o arquivo local não existir
            ClientError: Em caso de erro na API da AWS
        """
        raise NotImplementedError("O método 'upload_object' não implementado.")
