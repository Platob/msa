__all__ = ["PyODBCCursor"]

from typing import Optional, Any

from pyarrow import schema, Schema
from pyodbc import Cursor

from msa.config import DEFAULT_BATCH_ROW_SIZE
from msa.cursor import Cursor as AbstractCursor
from msa.utils import pyodbc_description_to_pyarrow_field


class PyODBCCursor(AbstractCursor):

    @property
    def schema_arrow(self) -> Schema:
        if self.__schema is None:
            self.__schema = schema(
                [pyodbc_description_to_pyarrow_field(_) for _ in self.raw.description]
            )
        return self.__schema

    def __init__(
        self,
        connection: "PyODBCConnection",
        raw: Cursor,
        fast_executemany: bool = True,
        nocount: bool = True
    ):
        self.raw = raw
        self.raw.fast_executemany = fast_executemany

        super(PyODBCCursor, self).__init__(connection=connection, nocount=nocount)

        self.__schema = None

    def __del__(self):
        self.close()

    @property
    def fast_executemany(self):
        return self.raw.fast_executemany

    @fast_executemany.setter
    def fast_executemany(self, fast_executemany: bool):
        self.raw.fast_executemany = fast_executemany

    def close(self) -> None:
        if not self.closed:
            # self.raw.close() pyodbc close it
            super(PyODBCCursor, self).close()

    def execute(self, sql: str, *args, **kwargs) -> "PyODBCCursor":
        self.raw.execute(sql, *args, **kwargs)
        return self

    def executemany(self, sql: str, *args, **kwargs) -> "PyODBCCursor":
        self.raw.executemany(sql, *args, **kwargs)
        return self

    def fetchone(self) -> Optional[tuple[Any]]:
        return self.raw.fetchone()

    def fetchmany(self, n: int = DEFAULT_BATCH_ROW_SIZE) -> Optional[list[tuple[Any]]]:
        return self.raw.fetchmany(n)

    def fetchall(self, buffersize: int = 10) -> list[tuple[Any]]:
        return self.raw.fetchall()
