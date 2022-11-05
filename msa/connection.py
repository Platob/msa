__all__ = ["Connection"]

from abc import ABC, abstractmethod
from typing import Optional

from msa.cursor import Cursor
from msa.table import SQLTable


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

    @abstractmethod
    def commit(self) -> None:
        raise NotImplemented

    @abstractmethod
    def rollback(self) -> None:
        raise NotImplemented

    @abstractmethod
    def cursor(self) -> Cursor:
        raise NotImplemented

    # table
    def tables(self, catalog: str = "master", schema: str = "dbo", expression: Optional[str] = None):
        stmt = "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE " \
               "TABLE_CATALOG = '%s' AND TABLE_SCHEMA = '%s'" % (catalog, schema)
        if expression:
            stmt += " AND TABLE_NAME LIKE '%s'" % expression
        with self.cursor() as c:
            for row in c.execute(stmt).fetchall():
                yield SQLTable(self, catalog=row[0], schema=row[1], name=row[2], type=row[3])

    def table(self, name: str, catalog: str = "master", schema: str = "dbo"):
        for t in self.tables(catalog, schema, name):
            return t
        raise ValueError("Cannot find table '%s'" % name)
