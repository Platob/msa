__all__ = ["PyODBCCursor"]

from typing import Optional

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

    def __init__(self, connection: "PyODBCConnection", raw: Cursor):
        super(PyODBCCursor, self).__init__(connection=connection)
        self.raw = raw

        self.__schema = None

    def __del__(self):
        self.close()

    def close(self) -> None:
        if not self.closed:
            self.raw.close()
        super(PyODBCCursor, self).close()

    def execute(self, sql: str, *args, **kwargs) -> "PyODBCCursor":
        self.raw.execute(sql, *args, **kwargs)
        return self

    def executemany(self, sql: str, *args, **kwargs) -> "PyODBCCursor":
        self.raw.executemany(sql, *args, **kwargs)
        return self

    def fetchone(self) -> Optional[tuple[object]]:
        return self.raw.fetchone()

    def fetchmany(self, n: int = DEFAULT_BATCH_ROW_SIZE) -> Optional[list[tuple[object]]]:
        return self.raw.fetchmany(n)

    def fetchall(self, buffersize: int = 10) -> list[tuple[object]]:
        return self.raw.fetchall()
