__all__ = ["SQLTable"]

from typing import Optional, Any, Union, Iterable

from pyarrow import Schema, schema, Field, RecordBatch, Table, RecordBatchReader

from .config import DEFAULT_SAFE_MODE
from .cursor import Cursor
from .utils import mssql_column_to_pyarrow_field
from .utils.arrow import cast_arrow


class SQLTable:

    def __init__(
        self,
        connection: "Connection",
        catalog: str,
        schema: str,
        name: str,
        type: str,
        schema_arrow: Optional[Schema] = None
    ):
        self.__schema_arrow = schema_arrow

        self.connection = connection
        self.catalog = catalog
        self.schema = schema
        self.name = name
        self.type = type

    def __repr__(self):
        return "SQLTable('%s', '%s', '%s')" % (
            self.catalog, self.schema, self.name
        )

    def __str__(self):
        return "[%s].[%s].[%s]" % (
            self.catalog, self.schema, self.name
        )

    def __getitem__(self, item):
        return self.field(item)

    @property
    def schema_arrow(self) -> Schema:
        if self.__schema_arrow is None:
            self.__schema_arrow = schema(
                [
                    mssql_column_to_pyarrow_field(row)
                    for row in self.connection.cursor().execute("""select col.name as name,
    t.name as dtype, t.max_length as max_length, t.precision as precision, t.scale as scale,
    t.is_nullable as nullable, t.collation_name as collation
from sys.tables as tab
    inner join sys.columns as col on tab.object_id = col.object_id
    left join sys.types as t on col.user_type_id = t.user_type_id
where schema_name(tab.schema_id) = '%s' and tab.name = '%s'""" % (self.schema, self.name)).fetchall()
                ],
                metadata={
                    "engine": "mssql",
                    "catalog": self.catalog,
                    "schema": self.schema,
                    "type": self.type
                }
            )
        return self.__schema_arrow

    def field(self, name: str) -> Field:
        try:
            try:
                return self.schema_arrow.field(self.schema_arrow.names.index(name))
            except ValueError as e:
                for field in self.schema_arrow:
                    if field.name.lower() == name.lower():
                        return field
                raise e
        except ValueError:
            raise ValueError("%s: Cannot find column '%s' in %s" % (
                repr(self), name, self.schema_arrow.names
            ))

    def truncate(self):
        with self.connection.cursor() as c:
            c.execute(f"TRUNCATE TABLE [%s].[%s].[%s]" % (
                self.catalog, self.schema, self.name
            ))
            c.commit()

    def drop(self):
        with self.connection.cursor() as c:
            c.execute(f"DROP TABLE [%s].[%s].[%s]" % (
                self.catalog, self.schema, self.name
            ))
            c.commit()

    def count(self) -> int:
        with self.connection.cursor() as c:
            return c.execute(f"select count(*) from [%s].[%s].[%s]" % (
                self.catalog, self.schema, self.name
            )).fetchall()[0][0]

    def prepare_insert_statement(self, columns):
        return "INSERT INTO [%s].[%s].[%s] ([%s]) VALUES (%s)" % (
            self.catalog, self.schema, self.name, "],[".join(columns),
            ",".join(("?" for _ in range(len(columns))))
        )

    def insert_pylist(
        self,
        rows: list[Union[list[Any], tuple[Any]]],
        columns: list[str],
        fast_executemany: bool = True,
        stmt: str = "",
        commit: bool = True,
        cursor: Optional[Cursor] = None
    ):
        if rows:
            if cursor:
                try:
                    cursor.fast_executemany = fast_executemany
                except AttributeError:
                    pass

                cursor.executemany(stmt if stmt else self.prepare_insert_statement(columns), rows)

                if commit:
                    cursor.commit()
            else:
                with self.connection.cursor() as c:
                    return self.insert_pylist(
                        rows,
                        columns,
                        fast_executemany,
                        stmt,
                        commit=commit,
                        cursor=c
                    )

    def insert_arrow(
        self,
        data: Union[
            RecordBatch, Table,
            Iterable[RecordBatch], RecordBatchReader
        ],
        cast: bool = True,
        safe: bool = DEFAULT_SAFE_MODE,
        fast_executemany: bool = True,
        stmt: str = "",
        commit: bool = True,
        cursor: Optional[Cursor] = None
    ):
        if cast:
            data = cast_arrow(data, self.schema_arrow, safe)
        elif isinstance(data, Table):
            data = RecordBatchReader.from_batches(data.schema, data.to_batches())

        if isinstance(data, RecordBatch):
            return self.insert_pylist(
                [tuple(row.values()) for row in data.to_pylist()],
                data.schema.names,
                fast_executemany=fast_executemany,
                stmt=stmt,
                commit=commit,
                cursor=cursor
            )
        else:
            stmt = self.prepare_insert_statement(data.schema.names)

            for batch in data:
                self.insert_pylist(
                    [tuple(row.values()) for row in batch.to_pylist()],
                    data.schema.names,
                    fast_executemany=fast_executemany,
                    stmt=stmt,
                    commit=commit,
                    cursor=cursor
                )
