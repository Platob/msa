__all__ = ["Cursor"]

from abc import ABC, abstractmethod
from typing import Optional, Generator, Any, Union

from pyarrow import Schema, RecordBatch, Table, RecordBatchReader, array

from msa.config import DEFAULT_BATCH_ROW_SIZE, DEFAULT_SAFE_MODE
from msa.utils import prepare_insert_batch_statement, prepare_insert_statement


def linearize(it):
    for _0 in it:
        for _1 in _0:
            yield _1


def concat_rows(rows, batch_size: int = 1):
    while len(rows) > batch_size:
        resized, rows = rows[:batch_size], rows[batch_size:]
        yield list(linearize(resized))
    if len(rows) > 0:
        yield list(linearize(rows))


class Cursor(ABC):

    @staticmethod
    def safe_commit_size(commit_size: int, columns_len: int):
        return int(min(columns_len * commit_size, 2099) / columns_len)

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

    @property
    def fast_executemany(self):
        return False

    @fast_executemany.setter
    def fast_executemany(self, fast_executemany: bool):
        pass

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

    # Inserts
    # INSERT INTO VALUES
    def insert_pylist(
        self,
        table: Union[str, "SQLTable"],
        rows: list[Union[list[Any], tuple[Any]]],
        columns: list[str],
        stmt: str = "",
        commit: bool = True,
        tablock: bool = False,
        commit_size: int = 1
    ):
        """
        Insert values like list[list[Any]]

        :param table: table name or msq.SQLTable
        :param rows: row batch: list[list[Any]]
        :param columns: column names
        :param stmt:
        :param commit: commit at the end
        :param tablock: see self.prepare_insert_statement or self.prepare_insert_batch_statement
        :param commit_size: number of row batch in insert values
            Must be 0 > batch_size > 1001 and len(columns) * commit_size <= 2100
        :return: None
        """
        if rows:
            if commit_size > 1:
                # https://stackoverflow.com/questions/845931/maximum-number-of-parameters-in-sql-query/845972#845972
                # Maximum < 2100 parameters
                commit_size = self.safe_commit_size(commit_size, len(columns))

                stmt = stmt if stmt else prepare_insert_batch_statement(
                    table, columns, tablock=tablock, commit_size=commit_size
                )

                rows: list[list] = list(concat_rows(rows, commit_size))

                if len(rows) == 1:
                    last = rows[0]
                else:
                    last = rows[-1]
                    self.executemany(stmt, rows[:-1])

                self.executemany(
                    prepare_insert_batch_statement(
                        table, columns, tablock=tablock, commit_size=int(len(last) / len(columns))
                    ),
                    [last]
                )
            else:
                self.executemany(
                    stmt if stmt else prepare_insert_statement(table, columns, tablock=tablock),
                    rows
                )

            if commit:
                self.commit()

    # config statements
    def set_identity_insert(self, table: "msa.table.SQLTable", on: bool = True):
        self.execute("SET IDENTITY_INSERT %s %s" % (table.full_name, "ON" if on else "OFF"))

    def set_nocount(self, on: bool = True):
        self.execute("SET NOCOUNT %s" % 'ON' if on else 'OFF')

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
