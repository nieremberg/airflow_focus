"""
Este módulo fornece uma implementação da interface StorageInterface para 
interagir com o armazenamento OBS da Huawei Pública.
"""
import os
from obs import ObsClient
from airflow.sdk import Variable
from interfaces.storage_interface import StorageInterface


class HPCStorage(StorageInterface):
    def __init__(self, bucket):
        self.bucket = bucket
        self.max_keys = 1000
        self.client = ObsClient(
            access_key_id=Variable.get("HPC_ACCESS_KEY"),
            secret_access_key=Variable.get("HPC_SECRET_KEY"),
            server=Variable.get("HPC_SERVER"),
        )

    def list_objects(self, prefix=""):
        objects = []
        marker = None

        while True:
            response = self.client.listObjects(
                bucketName=self.bucket,
                prefix=prefix,
                marker=marker,
                max_keys=self.max_keys,
                encoding_type="url",
            )

            if response.status >= 300:
                raise Exception(
                    f"Erro ao listar objetos: {response.errorCode} - {response.errorMessage}"
                )

            objects.extend(
                [obj.key for obj in response.body.contents if obj.key.endswith(".zip")]
            )

            # se ainda tem mais páginas, atualiza o marker e continua
            if response.body.is_truncated:
                marker = response.body.next_marker
            else:
                break

        return objects

    def download_object(self, key, destination):
        """
        Faz o download de um objeto do OBS usando getObject (download simples).
        Escreve o conteúdo explicitamente no disco, garantindo que o arquivo
        seja gravado corretamente (análogo ao método usado pelo OCI Storage).
        """
        os.makedirs(os.path.dirname(destination), exist_ok=True)

        response = self.client.getObject(self.bucket, key)

        if response.status >= 300:
            raise Exception(
                f"Erro ao baixar objeto {key}: {response.errorCode} - {response.errorMessage}"
            )

        # Escreve o conteúdo no disco em chunks para suportar arquivos grandes
        with open(destination, 'wb') as f:
            while True:
                chunk = response.body.response.read(65536)
                if not chunk:
                    break
                f.write(chunk)
        response.body.response.close()

        # Verifica se o arquivo foi realmente criado
        if not os.path.exists(destination):
            raise FileNotFoundError(
                f"Arquivo {destination} não foi criado após o download de {key}"
            )

    def upload_object(self, source, key):
        raise NotImplementedError("O método 'upload_object' não implementado.")


class HPCFocusStorage(HPCStorage):
    def list_objects(self, prefix=""):
        objects = []
        marker = None

        while True:
            response = self.client.listObjects(
                bucketName=self.bucket,
                prefix=prefix,
                marker=marker,
                max_keys=self.max_keys,
                encoding_type="url",
            )

            if response.status >= 300:
                raise Exception(
                    f"Erro ao listar objetos: {response.errorCode} - {response.errorMessage}"
                )

            # Check if contents is not None before iterating
            if response.body.contents:
                objects.extend(
                    [obj.key for obj in response.body.contents if obj.key.endswith(".zip")]
                )

            # se ainda tem mais páginas, atualiza o marker e continua
            if response.body.is_truncated:
                marker = response.body.next_marker
            else:
                break

        return objects
