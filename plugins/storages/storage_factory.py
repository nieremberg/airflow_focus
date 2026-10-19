"""
Define uma fábrica para lidar os storages de objetos e retorna uma instância
apropriada com base no provedor especificado.
"""
from storages.aws_storage import AWSStorage
from storages.oci_storage import OCIStorage
from storages.ecs_storage import DellECSStorage
from storages.hpc_storage import HPCStorage, HPCFocusStorage
from storages.gcp_storage import GCPStorage

def get_storage(provider: str, **kwargs):
    """
    Retorna uma instância de armazenamento com base no provedor especificado.

    Args:
        provider (str): Sigla do provedor de armazenamento. Pode ser um dos seguintes:
            - "aws": Para armazenamento AWS S3
            - "oci": Para armazenamento Oracle Cloud Infrastructure
            - "ecs": Para backup storage (Dell ECS, MinIO, S3-compatible)
            - "hpc": Para armazenamento Huawei Cloud (SDR)
            - "hpc_focus": Para armazenamento Huawei Cloud (FOCUS)
            - "gcp": Para armazenamento Google Cloud Storage
        **kwargs: Argumentos adicionais necessários para inicializar o armazenamento.
            - bucket (str): O nome do bucket de armazenamento.
    Returns:
        Uma instância da classe de armazenamento correspondente ao provedor especificado.
    Raises:
        ValueError: Se o provedor especificado não for suportado.
    """
    if provider == "aws":
        return AWSStorage(bucket=kwargs["bucket"])
    elif provider == "oci":
        return OCIStorage(bucket=kwargs["bucket"])
    elif provider == "ecs":
        return DellECSStorage(bucket=kwargs["bucket"])
    elif provider == "hpc":
        return HPCStorage(bucket=kwargs["bucket"])
    elif provider == "hpc_focus":
        return HPCFocusStorage(bucket=kwargs["bucket"])
    elif provider == "gcp":
        return GCPStorage(bucket=kwargs["bucket"])
    else:
        raise ValueError(f"Storage provider '{provider}' não é suportado.")
