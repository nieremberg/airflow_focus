""" Define a interface genérica para o armazenamento de dados """

from abc import ABC, abstractmethod

class StorageInterface(ABC):
    """_summary_

    Args:
        ABC (_type_): _description_
    """
    @abstractmethod
    def list_objects(self, prefix: str = "") -> list[str]:
        """List objects in the storage with an optional prefix."""
        pass

    @abstractmethod
    def download_object(self, key: str, destination: str) -> None:
        """Download an object from the storage."""
        pass

    @abstractmethod
    def upload_object(self, source: str, key: str) -> None:
        """Upload an object to the storage."""
        pass
