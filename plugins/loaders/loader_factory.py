from loaders.aws_loader import AWSLoader
from loaders.oci_loader import OCILoader
from loaders.hpc_loader import HPCLoader, HPCFocusLoader
from loaders.gcp_loader import GCPLoader

def get_loader(provider, temp_dir):
    """
    Factory para criar loaders específicos por provedor.

    Args:
        provider: Nome do provedor (aws, oci, gcp, hpc, hpc_focus)
        temp_dir: Diretório temporário para processamento

    Returns:
        Instância do loader apropriado

    Raises:
        ValueError: Se o provider não for suportado
    """
    if provider == 'aws':
        return AWSLoader(provider, temp_dir)
    elif provider == 'oci':
        return OCILoader(provider, temp_dir)
    elif provider == 'hpc':
        return HPCLoader(provider, temp_dir)
    elif provider == 'hpc_focus':
        return HPCFocusLoader(provider, temp_dir)
    elif provider == 'gcp':
        return GCPLoader(provider, temp_dir)
    else:
        raise ValueError(f"Loader para o provider '{provider}' não implementado.")