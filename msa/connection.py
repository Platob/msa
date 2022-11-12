__all__ = ["Connection"]

from abc import ABC, abstractmethod
from typing import Optional

from .cursor import Cursor
from .table import SQLTable, SQLView


class Connection(ABC):

    def __init__(self, server: "msa.server.MSSQL"):
        self.server = server
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __bool__(self):
        return not self.closed

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
    def tables(self, catalog: str = "%%", schema: str = "%%", expression: Optional[str] = None):
        stmt = "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, OBJECT_ID('[' + TABLE_CATALOG + '].[' + " \
               "TABLE_SCHEMA + '].[' + TABLE_NAME + ']') as oid FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG " \
               "LIKE '%s' AND TABLE_SCHEMA LIKE '%s'" % (catalog, schema)
        if expression:
            stmt += " AND TABLE_NAME LIKE '%s'" % expression
        with self.cursor() as c:
            for row in c.execute(stmt).fetchall():
                yield SQLTable(self, catalog=row[0], schema=row[1], name=row[2], type=row[3], object_id=row[4])

    def table(self, name: str, catalog: str = "master", schema: str = "dbo"):
        for t in self.tables(catalog, schema, name):
            return t
        raise ValueError("Cannot find table [%s].[%s].[%s]" % (catalog, schema, name))

    def views(self, catalog: str = "%%", schema: str = "%%", expression: Optional[str] = None):
        stmt = "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, OBJECT_ID('[' + TABLE_CATALOG + '].[' + " \
               "TABLE_SCHEMA + '].[' + TABLE_NAME + ']') as oid FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG " \
               "LIKE '%s' AND TABLE_SCHEMA LIKE '%s'" % (catalog, schema)
        if expression:
            stmt += " AND TABLE_NAME LIKE '%s'" % expression
        with self.cursor() as c:
            for row in c.execute(stmt).fetchall():
                yield SQLView(self, catalog=row[0], schema=row[1], name=row[2], type="VIEW", object_id=row[3])

    def view(self, name: str, catalog: str = "master", schema: str = "dbo"):
        for t in self.views(catalog, schema, name):
            return t
        raise ValueError("Cannot find view [%s].[%s].[%s]" % (catalog, schema, name))

    def table_or_view(self, name: str, catalog: str = "%%", schema: str = "%%"):
        for t in self.tables(catalog, schema, name):
            return t
        for t in self.views(catalog, schema, name):
            return t
        raise ValueError("Cannot find table / view [%s].[%s].[%s]" % (catalog, schema, name))
