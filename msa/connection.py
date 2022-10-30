__all__ = ["Connection"]

from abc import ABC, abstractmethod

from msa.cursor import Cursor


class Connection(ABC):

    def __init__(self, server: "msa.server.MSSQL"):
        self.server = server
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self) -> None:
        self.closed = True

    def commit(self) -> None:
        pass

    @abstractmethod
    def cursor(self) -> Cursor:
        raise NotImplemented
