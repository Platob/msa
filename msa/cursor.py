__all__ = ["Cursor"]

from abc import ABC, abstractmethod
from typing import Optional, Generator, Any

from pyarrow import Schema, RecordBatch, Table, RecordBatchReader, array

from msa.config import DEFAULT_BATCH_ROW_SIZE, DEFAULT_SAFE_MODE


class Cursor(ABC):

    def __init__(self, connection: "Connection"):
        self.closed = False

        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __bool__(self):
        return not self.closed

    @property
    def server(self) -> "MSSQL":
        return self.connection.server

    @property
    @abstractmethod
    def schema_arrow(self) -> Schema:
        raise NotImplemented

    def close(self) -> None:
        self.closed = True

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    @abstractmethod
    def execute(self, sql: str, *args, **kwargs) -> "Cursor":
        raise NotImplemented

    @abstractmethod
    def executemany(self, sql: str, *args, **kwargs) -> "Cursor":
        raise NotImplemented

    # row oriented
    @abstractmethod
    def fetchone(self) -> Optional[tuple[object]]:
        raise NotImplemented

    @abstractmethod
    def fetchmany(self, n: int = DEFAULT_BATCH_ROW_SIZE) -> Optional[list[tuple[Any]]]:
        raise NotImplemented

    def fetchall(self, buffersize: int = DEFAULT_BATCH_ROW_SIZE) -> list[tuple[Any]]:
        buf = []
        for rows in self.fetch_row_batches(buffersize):
            buf.extend(rows)
        return buf

    def fetch_row_batches(self, n: int) -> Generator[list[tuple[object]], None, None]:
        while 1:
            rows = self.fetchmany(n)
            if not rows:
                break
            yield rows
    
    def rows(self, buffersize: int = DEFAULT_BATCH_ROW_SIZE) -> Generator[tuple[object], None, None]:
        for batch in self.fetch_row_batches(buffersize):
            for row in batch:
                yield row
    
    # column oriented
    def fetch_arrow_batches(
        self,
        n: int = DEFAULT_BATCH_ROW_SIZE,
        safe: bool = DEFAULT_SAFE_MODE
    ) -> Generator[RecordBatch, None, None]:
        empty = True

        for batch in self.fetch_row_batches(n):
            yield RecordBatch.from_arrays(
                [
                    array(
                        [row[i] for row in batch],
                        self.schema_arrow[i].type,
                        from_pandas=False,
                        safe=safe
                    )
                    for i in range(len(self.schema_arrow))
                ],
                schema=self.schema_arrow
            )

            empty = False

        if empty:
            yield RecordBatch.from_pylist([], schema=self.schema_arrow)

    def fetch_arrow(
        self,
        n: int = DEFAULT_BATCH_ROW_SIZE,
        safe: bool = DEFAULT_SAFE_MODE
    ) -> Table:
        return Table.from_batches(self.fetch_arrow_batches(n, safe), schema=self.schema_arrow)

    def reader(
        self,
        n: int = DEFAULT_BATCH_ROW_SIZE,
        safe: bool = DEFAULT_SAFE_MODE
    ) -> RecordBatchReader:
        return RecordBatchReader.from_batches(self.schema_arrow, self.fetch_arrow_batches(n, safe))

    # config statements
    def set_identity_insert(self, table: "msa.table.SQLTable", on: bool = True):
        self.execute("SET IDENTITY_INSERT %s %s" % (table.full_name, "ON" if on else "OFF"))

    def create_table_index(
        self,
        table: "msa.table.SQLTable",
        name: str = "",
        type: str = "",
        columns: list[str] = ()
    ):
        """
        See https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql?view=sql-server-ver16

        "CREATE %s INDEX [%s] ON %s.%s.%s (%s)" % (
            type,
            name if name else "IDX:%s" % ("_".join(columns)),
            table.catalog, table.schema, table.name,
            ",".join(columns)
        )
        :param table: msa.table.SQLTable
        :param name: index name
        :param type:
        :param columns: column names
        """
        self.execute(
            "CREATE %s INDEX [%s] ON %s.%s.%s (%s)" % (
                type,
                name if name else "IDX:%s" % ("_".join(columns)),
                table.catalog, table.schema, table.name,
                ",".join(columns)
            )
        )
        self.commit()

    def drop_table_index(self, table: "msa.table.SQLTable", name: str):
        self.execute("DROP INDEX [%s] ON %s" % (name, table.full_name))
        self.commit()

    def disable_table_index(self, table: "msa.table.SQLTable", name: str):
        self.execute("ALTER INDEX [%s] ON %s DISABLE" % (name, table.full_name))
        self.commit()

    def disable_table_all_indexes(self, table: "msa.table.SQLTable"):
        self.execute("ALTER INDEX ALL ON %s DISABLE" % table.full_name)
        self.commit()

    def rebuild_table_index(self, table: "msa.table.SQLTable", name: str):
        self.execute("ALTER INDEX [%s] ON %s REBUILD" % (name, table.full_name))
        self.commit()

    def rebuild_table_all_indexes(self, table: "msa.table.SQLTable"):
        self.execute("ALTER INDEX ALL ON %s REBUILD" % table.full_name)
        self.commit()
