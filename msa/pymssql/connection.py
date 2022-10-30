__all__ = ["PyMSSQLConnection"]

import pymssql

from msa import Connection as Abstract
from .cursor import PyMSSQLCursor


class PyMSSQLConnection(Abstract):

    def __init__(self, server: "PyMSSQL", raw: pymssql.Connection):
        super().__init__(server)
        self.raw = raw

    def close(self):
        if not self.closed:
            super(PyMSSQLConnection, self).close()
        self.raw.close()

    def commit(self) -> None:
        super(PyMSSQLConnection, self).commit()
        self.raw.commit()

    def cursor(self, *args, **kwargs) -> PyMSSQLCursor:
        return PyMSSQLCursor(
            self,
            self.raw.cursor(*args, **kwargs)
        )
