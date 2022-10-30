__all__ = ["PyODBCConnection"]

import pymssql

from msa import Connection as Abstract
from .cursor import PyODBCCursor


class PyODBCConnection(Abstract):

    def __init__(self, server: "PyMSSQL", raw: pymssql.Connection):
        super().__init__(server)
        self.raw = raw

    def close(self):
        if not self.closed:
            self.raw.close()
        super(PyODBCConnection, self).close()

    def commit(self) -> None:
        super(PyODBCConnection, self).commit()
        self.raw.commit()

    def cursor(self, *args, **kwargs) -> PyODBCCursor:
        return PyODBCCursor(
            self,
            self.raw.cursor(*args, **kwargs)
        )
