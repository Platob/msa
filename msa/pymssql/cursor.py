__all__ = ["PyMSSQLCursor"]

from typing import Optional

import pyarrow
import pymssql
from pyarrow import field, Schema

from msa.config import DEFAULT_BATCH_ROW_SIZE
from msa.cursor import Cursor as AbstractCursor


class PyMSSQLCursor(AbstractCursor):

    @property
    def schema_arrow(self) -> Schema:
        if self.__schema is None:
            self.__schema = [field(_[0], pyarrow.string()) for _ in self.raw.description]
        return self.__schema

    def __init__(self, connection: "PyMSSQLConnection", raw: pymssql.Cursor):
        super(PyMSSQLCursor, self).__init__(connection=connection)
        self.raw = raw

        self.__schema = None

    def __del__(self):
        self.close()

    def close(self) -> None:
        if not self.closed:
            self.raw.close()
        super(PyMSSQLCursor, self).close()

    def execute(self, sql: str, *args, **kwargs) -> "PyMSSQLCursor":
        self.raw.execute(sql, *args, **kwargs)
        return self

    def executemany(self, sql: str, *args, **kwargs) -> "PyMSSQLCursor":
        self.raw.executemany(sql, *args, **kwargs)
        return self

    def fetchone(self) -> Optional[tuple[object]]:
        return self.raw.fetchone()

    def fetchmany(self, n: int = DEFAULT_BATCH_ROW_SIZE) -> Optional[list[tuple[object]]]:
        return self.raw.fetchmany(n)

    def fetchall(self, buffersize: int = DEFAULT_BATCH_ROW_SIZE) -> list[tuple[object]]:
        return self.raw.fetchall()
