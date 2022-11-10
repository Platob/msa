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
        self.raw.commit()

    def rollback(self) -> None:
        self.raw.rollback()

    def cursor(self, *args, **kwargs) -> PyMSSQLCursor:
        # reconnect if closed
        return self.server.connect().cursor(*args, **kwargs) if self.closed else PyMSSQLCursor(
            self,
            self.raw.cursor(*args, **kwargs)
        )
