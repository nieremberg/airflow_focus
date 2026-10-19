"""
Este módulo fornece uma implementação da interface StorageInterface para 
interagir com o armazenamento DELL ECS.
"""
import logging
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, BotoCoreError
from boto3.s3.transfer import TransferConfig
from airflow.sdk import Variable
from interfaces.storage_interface import StorageInterface

logger = logging.getLogger(__name__)

class DellECSStorage(StorageInterface):
    def __init__(self, bucket):
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=Variable.get("ECS_ACCESS_KEY"),
            aws_secret_access_key=Variable.get("ECS_SECRET_KEY"),
            endpoint_url=Variable.get("ECS_ENDPOINT_URL"),
        )
        self.s3_write = boto3.client(
            "s3",
            aws_access_key_id=Variable.get("ECS_ACCESS_KEY"),
            aws_secret_access_key=Variable.get("ECS_SECRET_KEY"),
            endpoint_url=Variable.get("ECS_ENDPOINT_URL"),
            config=Config(
                signature_version="s3v4",
                s3={
                    "addressing_style": "path",
                    "payload_signing_enabled": False,  # evita streaming SigV4/trailers
                },
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
                retries={"max_attempts": 3, "mode": "standard"},
                max_pool_connections=1,
                connect_timeout=60,
                read_timeout=300,
            ),
        )
        self.bucket = bucket
        self.transfer_config = TransferConfig(
            multipart_threshold=5 * 1024 * 1024,
            multipart_chunksize=5 * 1024 * 1024, #antes 16MB
            max_concurrency=1, 
            use_threads=False, 
        )

    def list_objects(self, prefix=""):
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        if "Contents" in response:
            return {
                obj["Key"]: obj["ETag"].strip('"')
                for obj in response["Contents"]
            }
        return {}

    def download_object(self, key, destination):
        self.s3.download_file(self.bucket, key, destination)

    def upload_object(self, source, key):
        logger.info(
            "Iniciando upload para Dell ECS: "
            "bucket=%s key=%s source=%s",
            self.bucket,
            key,
            source,
        )
        try:
            self.s3_write.upload_file(
                source, 
                self.bucket, 
                key, 
                Config=self.transfer_config, 
            )
            logger.info(
                "Upload concluído com sucesso: bucket=%s key=%s",
                self.bucket,
                key,
            )
        except ClientError as e:
            error = e.response.get("Error", {}) 
            metadata = e.response.get("ResponseMetadata", {}) 
            logger.error( 
                "Erro S3 durante upload para Dell ECS: " 
                "bucket=%s key=%s " 
                "code=%s message=%s " 
                "request_id=%s http_status=%s", 
                self.bucket, 
                key, 
                error.get("Code"), 
                error.get("Message"), 
                metadata.get("RequestId"), 
                metadata.get("HTTPStatusCode"), 
                exc_info=True, 
            )
            raise
            
        except BotoCoreError:
            logger.exception(
                "Erro BotoCore durante upload para Dell ECS: "
                "bucket=%s key=%s",
                self.bucket,
                key,
                exc_info=True,
            )

            raise

        except Exception:
            logger.exception(
                "Erro inesperado durante upload para Dell ECS: "
                "bucket=%s key=%s",
                self.bucket,
                key,
                exc_info=True,
            )

            raise
