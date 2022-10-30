__all__ = ["MSSQL"]

from abc import abstractmethod

from msa.connection import Connection
from msa.cursor import Cursor


class MSSQL:

    def __init__(
        self,
        package: str
    ):
        self.package = package

    @abstractmethod
    def connect(self, **kwargs) -> Connection:
        raise NotImplemented

    def cursor(self, **kwargs) -> Cursor:
        return self.connect(**kwargs).cursor()
