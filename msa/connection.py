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
        with self.cursor() as c:
            return c.tables(catalog, schema, expression)

    def table(self, name: str, catalog: str = "%%", schema: str = "%%"):
        with self.cursor() as c:
            return c.table(name, catalog, schema)

    def views(self, catalog: str = "%%", schema: str = "%%", expression: Optional[str] = None):
        with self.cursor() as c:
            return c.views(catalog, schema, expression)

    def view(self, name: str, catalog: str = "%%", schema: str = "%%"):
        with self.cursor() as c:
            return c.view(name, catalog, schema)

    def table_or_view(self, name: str, catalog: str = "%%", schema: str = "%%"):
        with self.cursor() as c:
            return c.table_or_view(name, catalog, schema)
