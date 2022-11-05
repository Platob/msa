__all__ = ["PyODBC"]

import pyodbc

from msa.server import MSSQL
from .connection import PyODBCConnection


class PyODBC(MSSQL):

    def __init__(
        self,
        uri: str
    ):
        super(PyODBC, self).__init__(package="pyodbc")
        self.uri = uri

    @property
    def uri(self) -> str:
        return self.__uri

    @uri.setter
    def uri(self, uri: str):
        self.__uri = uri
        self.uri_map = {
            t[0]: t[1] for t in (_.split("=") for _ in uri.split(";") if _)
        }

    def connect(self, **kwargs) -> PyODBCConnection:
        return PyODBCConnection(
            server=self,
            raw=pyodbc.connect(self.uri, **kwargs)
        )
