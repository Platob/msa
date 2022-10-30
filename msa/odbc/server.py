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

    def connect(self, **kwargs) -> PyODBCConnection:
        return PyODBCConnection(
            server=self,
            raw=pyodbc.connect(self.uri, **kwargs)
        )
