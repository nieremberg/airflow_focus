"""
Este módulo fornece uma implementação da interface StorageInterface para 
interagir com o armazenamento da OCI.
"""
import os
import oci
from airflow.sdk import Variable
from interfaces.storage_interface import StorageInterface

class OCIStorage(StorageInterface):
    def __init__(self, bucket):
        self.bucket = bucket
        self.namespace = Variable.get("OCI_NAMESPACE")
        self.client = oci.object_storage.ObjectStorageClient(oci.config.from_file("./config/oci.conf"))


    def list_objects(self, prefix=""):
        objects = []
        next_start_with = None

        while True:
            response = self.client.list_objects(
                namespace_name=self.namespace,
                bucket_name=self.bucket,
                prefix=prefix,
                start=next_start_with
            )
            objects.extend([obj.name for obj in response.data.objects])

            next_start_with = response.data.next_start_with
            if not next_start_with:
                break

        return objects


    def download_object(self, key, destination):
        response = self.client.get_object(self.namespace, self.bucket, key)
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        with open(destination, 'wb') as f:
            f.write(response.data.content)

    def upload_object(self, source, key):
        raise NotImplementedError("O método 'upload_object' não implementado.")