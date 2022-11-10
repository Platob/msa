__all__ = ["SQLTable", "SQLView", "SQLIndex"]

import pathlib
from pathlib import Path
from typing import Optional, Any, Union, Iterable, Generator

from pyarrow import Schema, schema, Field, RecordBatch, Table, RecordBatchReader, NativeFile
from pyarrow.fs import FileSystem, LocalFileSystem, FileSelector, FileType, FileInfo

from .utils.typing import ArrowData

try:
    from pyarrow.parquet import ParquetFile
except ImportError:
    class ParquetFile:
        def __enter__(self):
            pass

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

from .config import DEFAULT_SAFE_MODE, DEFAULT_ARROW_BATCH_ROW_SIZE
from .cursor import Cursor
from .utils import mssql_column_to_pyarrow_field, prepare_insert_statement, prepare_insert_batch_statement
from .utils.arrow import intersect_schemas

TABLE_TYPE_COLUMNS_STATEMENT = {
    "BASE TABLE": """select col.name as name,
    type_name(user_type_id) as dtype, col.max_length as max_length, col.precision as precision, col.scale as scale,
    col.is_nullable as nullable, col.collation_name as collation, col.is_identity as [identity]
from sys.tables as tab
inner join sys.columns as col on tab.object_id = col.object_id
where tab.object_id = %s""",
    "VIEW": """select c.name as name,
    type_name(user_type_id) as dtype, c.max_length, c.precision, c.scale, c.is_nullable as nullable,
    c.collation_name as collation, CAST(0 AS BIT) as [identity]
from sys.columns c
join sys.views v on v.object_id = %s and v.object_id = c.object_id"""
}


class SQLTable:

    def __init__(
        self,
        connection: "msa.Connection",
        catalog: str,
        schema: str,
        name: str,
        type: str,
        object_id: int,
        schema_arrow: Optional[Schema] = None
    ):
        self.__schema_arrow = schema_arrow

        self.connection = connection
        self.catalog = catalog
        self.schema = schema
        self.name = name
        self.type = type
        self.object_id = object_id

    def __hash__(self):
        return self.object_id

    def __eq__(self, other):
        if isinstance(other, SQLTable):
            return self.__hash__() == other.__hash__()
        return False

    def __repr__(self):
        return "%s('%s', '%s', '%s')" % (
            self.__class__.__name__,
            self.catalog, self.schema, self.name
        )

    def __str__(self):
        return self.full_name

    @property
    def full_name(self):
        return "[%s].[%s].[%s]" % (
            self.catalog, self.schema, self.name
        )

    def __getitem__(self, item):
        return self.field(item)

    @property
    def connection(self):
        return self._connection

    @connection.setter
    def connection(self, connection):
        from .connection import Connection
        self._connection: Connection = connection

    @property
    def schema_arrow(self) -> Schema:
        if self.__schema_arrow is None:
            self.__schema_arrow = schema(
                [
                    mssql_column_to_pyarrow_field(row, column_type="table.column")
                    for row in self.connection.cursor().execute(
                        TABLE_TYPE_COLUMNS_STATEMENT[self.type] % self.object_id
                    ).fetchall()
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

    @property
    def indexes(self) -> dict[str, "SQLIndex"]:
        with self.connection.cursor() as cursor:
            return {
                row[1]: SQLIndex(
                    self,
                    index_id=row[0],
                    name=row[1],
                    columns=row[2].split("||"),
                    type=row[3],
                    unique=row[4]
                )
                for row in cursor.execute("""select i.index_id, i.[name],
    substring(column_names, 1, len(column_names)-2) as [columns],
    i.type_desc,
    i.is_unique
from sys.objects t
inner join sys.indexes i on t.object_id = %s and t.object_id = i.object_id
cross apply (select col.[name] + '||' from sys.index_columns ic inner join sys.columns col
    on ic.object_id = col.object_id
    and ic.column_id = col.column_id
where ic.object_id = t.object_id
    and ic.index_id = i.index_id
    order by key_ordinal
    for xml path ('') ) D (column_names)
where t.is_ms_shipped <> 1
and index_id > 0""" % self.object_id).fetchall()
            }

    def truncate(self):
        with self.connection.cursor() as c:
            c.execute(f"TRUNCATE TABLE %s" % self.full_name)
            c.commit()

    def drop(self):
        with self.connection.cursor() as c:
            c.execute(f"DROP TABLE %s" % self.full_name)
            c.commit()

    def count(self) -> int:
        with self.connection.cursor() as c:
            return c.execute(f"select count(*) from %s" % self.full_name).fetchall()[0][0]

    def insert_pylist(
        self,
        rows: list[Union[list[Any], tuple[Any]]],
        columns: list[str],
        stmt: str = "",
        commit: bool = True,
        cursor: Optional[Cursor] = None,
        tablock: bool = False,
        commit_size: int = 1
    ):
        """
        Insert values like list[list[Any]]

        :param rows: row batch: list[list[Any]]
        :param columns: column names
        :param stmt:
        :param commit: commit at the end
        :param cursor: existing connection.Cursor, default will create new one
        :param tablock: see self.prepare_insert_statement or self.prepare_insert_batch_statement
        :param commit_size: number of row batch in insert values
            Must be 0 > batch_size > 1001 and len(columns) * commit_size <= 2100
        :return: None
        """
        if cursor is None:
            with self.connection.cursor() as c:
                return c.insert_pylist(
                    self,
                    rows,
                    columns,
                    stmt,
                    commit=commit,
                    tablock=tablock,
                    commit_size=commit_size
                )
        else:
            return cursor.insert_pylist(
                self,
                rows,
                columns,
                stmt,
                commit=commit,
                tablock=tablock,
                commit_size=commit_size
            )

    def insert_arrow(
        self,
        data: ArrowData,
        cast: bool = True,
        safe: bool = DEFAULT_SAFE_MODE,
        stmt: str = "",
        commit: bool = True,
        bulk: bool = False,
        cursor: Optional[Cursor] = None,
        tablock: bool = False,
        commit_size: int = 1,
        **insert_options: dict[str, Union[str, int, bool]]
    ):
        if cursor is None:
            with self.connection.cursor() as cursor:
                return cursor.insert_arrow(
                    self,
                    data,
                    cast=cast,
                    safe=safe,
                    stmt=stmt,
                    commit=commit,
                    bulk=bulk,
                    tablock=tablock,
                    commit_size=commit_size,
                    **insert_options
                )
        else:
            return cursor.insert_arrow(
                self,
                data,
                cast=cast,
                safe=safe,
                stmt=stmt,
                commit=commit,
                bulk=bulk,
                tablock=tablock,
                commit_size=commit_size,
                **insert_options
            )

    def bulk_insert_file(
        self,
        file: Union[str, Path],
        commit: bool = True,
        cursor: Optional[Cursor] = None,
        file_format: str = "CSV",
        field_terminator: str = ",",
        row_terminator: str = "\n",
        field_quote: str = '"',
        datafile_type: str = 'char',
        first_row: int = 2,
        code_page: str = '65001',
        tablock: bool = False,
        other_options: str = ""
    ):
        """
        See https://learn.microsoft.com/en-us/sql/t-sql/statements/bulk-insert-transact-sql?view=sql-server-2017

        :param file:
        :param commit:
        :param cursor:
        :param file_format:
        :param field_terminator: separator, default = ','
        :param row_terminator:
        :param field_quote:
        :param datafile_type:
        :param first_row:
        :param code_page: encoding, default '65001' = utf-8
        :param tablock: use TABLOCK options
        :param other_options: string to append: WITH (FORMAT = 'file_format', %s) % other_options
        :return: None
        """
        if cursor is None:
            with self.connection.cursor() as c:
                return c.bulk_insert_file(
                    self,
                    file,
                    commit,
                    file_format,
                    field_terminator=field_terminator,
                    row_terminator=row_terminator,
                    field_quote=field_quote,
                    datafile_type=datafile_type,
                    first_row=first_row,
                    code_page=code_page,
                    tablock=tablock,
                    other_options=other_options
                )
        else:
            cursor.bulk_insert_file(
                self,
                file,
                commit,
                file_format,
                field_terminator=field_terminator,
                row_terminator=row_terminator,
                field_quote=field_quote,
                datafile_type=datafile_type,
                first_row=first_row,
                code_page=code_page,
                tablock=tablock,
                other_options=other_options
            )

    def bulk_insert_arrow(
        self,
        data: ArrowData,
        base_dir: Optional[str] = None,
        cast: bool = True,
        safe: bool = DEFAULT_SAFE_MODE,
        commit: bool = True,
        cursor: Optional[Cursor] = None,
        delimiter: str = ",",
        file_format: str = "CSV",
        tablock: bool = False,
        **bulk_options: dict[str, str]
    ):
        if cursor is None:
            with self.connection.cursor() as cursor:
                return cursor.bulk_insert_arrow(
                    self,
                    data,
                    base_dir=base_dir,
                    cast=cast,
                    safe=safe,
                    commit=commit,
                    delimiter=delimiter,
                    file_format=file_format,
                    tablock=tablock,
                    **bulk_options
                )
        else:
            cursor.bulk_insert_arrow(
                self,
                data,
                base_dir=base_dir,
                cast=cast,
                safe=safe,
                commit=commit,
                delimiter=delimiter,
                file_format=file_format,
                tablock=tablock,
                **bulk_options
            )

    # extensions
    # Parquet
    def insert_parquet_file(
        self,
        source: Union[ParquetFile, str, pathlib.Path, NativeFile],
        batch_size: int = DEFAULT_ARROW_BATCH_ROW_SIZE,
        buffer_size: int = 0,
        cast: bool = True,
        safe: bool = DEFAULT_SAFE_MODE,
        commit: bool = True,
        cursor: Optional[Cursor] = None,
        filesystem: FileSystem = LocalFileSystem(),
        coerce_int96_timestamp_unit: str = None,
        use_threads: bool = True,
        **insert_arrow: dict
    ):
        """
        Insert data from parquet file with pyarrow methods

        :param source: pyarrow.parquet.ParquetFile, or something to build it
            See https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html#pyarrow.parquet.ParquetFile
        :param batch_size:
        :param buffer_size:
        :param cast:
        :param safe:
        :param commit:
        :param cursor:
        :param filesystem:
        :param coerce_int96_timestamp_unit:
        :param use_threads:
        :param insert_arrow: self.insert_arrow options
        :return: insert_arrow rtype
        """
        if cursor is None:
            with self.connection.cursor() as c:
                c.insert_parquet_file(
                    self,
                    source=source,
                    batch_size=batch_size,
                    buffer_size=buffer_size,
                    cast=cast,
                    safe=safe,
                    commit=commit,
                    filesystem=filesystem,
                    coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
                    use_threads=use_threads,
                    **insert_arrow
                )
        else:
            cursor.insert_parquet_file(
                self,
                source=source,
                batch_size=batch_size,
                buffer_size=buffer_size,
                cast=cast,
                safe=safe,
                commit=commit,
                filesystem=filesystem,
                coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
                use_threads=use_threads,
                **insert_arrow
            )

    def insert_parquet_dir(
        self,
        base_dir: str,
        recursive: bool = False,
        allow_not_found: bool = False,
        batch_size: int = DEFAULT_ARROW_BATCH_ROW_SIZE,
        buffer_size: int = 0,
        cast: bool = True,
        safe: bool = DEFAULT_SAFE_MODE,
        commit: bool = True,
        cursor: Optional[Cursor] = None,
        filesystem: FileSystem = LocalFileSystem(),
        coerce_int96_timestamp_unit: str = None,
        use_threads: bool = True,
        **insert_parquet_file_options: dict
    ) -> Generator[FileInfo, Any, None]:
        """
        Insert data from parquet iterating over files, sub dir files with pyarrow methods

        :param base_dir:
        :param recursive: recursive fetch files, set True to fetch all files in sub dirs
        :param allow_not_found:
        :param batch_size: number of rows for batch
        :param buffer_size:
        :param cast:
        :param safe:
        :param commit:
        :param cursor:
        :param filesystem: pyarrow FileSystem object
        :param coerce_int96_timestamp_unit:
        :param use_threads:
        :param insert_parquet_file_options: other cursor.insert_parquet_file options
        """
        for ofs in filesystem.get_file_info(
            FileSelector(base_dir, allow_not_found=allow_not_found, recursive=recursive)
        ):
            # <FileInfo for 'path': type=FileType.Directory>
            # or <FileInfo for 'path': type=FileType.File, size=0>
            if ofs.type == FileType.File:
                if ofs.size > 0:
                    self.insert_parquet_file(
                        source=ofs.path,
                        batch_size=batch_size,
                        buffer_size=buffer_size,
                        cast=cast,
                        safe=safe,
                        commit=commit,
                        cursor=cursor,
                        filesystem=filesystem,
                        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
                        use_threads=use_threads,
                        **insert_parquet_file_options
                    )
                    yield ofs
            elif ofs.type == FileType.Directory:
                for _ in self.insert_parquet_dir(
                    base_dir=ofs.path,
                    batch_size=batch_size,
                    buffer_size=buffer_size,
                    cast=cast,
                    safe=safe,
                    commit=commit,
                    cursor=cursor,
                    filesystem=filesystem,
                    coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
                    use_threads=use_threads,
                    **insert_parquet_file_options
                ):
                    yield _


class SQLView(SQLTable):

    def truncate(self):
        with self.connection.cursor() as c:
            c.execute(f"TRUNCATE VIEW [%s].[%s]" % (self.schema, self.name))
            c.commit()

    def drop(self):
        with self.connection.cursor() as c:
            c.execute(f"DROP VIEW [%s].[%s]" % (self.schema, self.name))
            c.commit()


class SQLIndex:

    def __init__(
        self,
        table: SQLTable,
        index_id: int,
        name: str,
        columns: list[str],
        type: str,
        unique: bool
    ):
        self.index_id = index_id
        self.table = table
        self.name = name
        self.columns = columns
        self.type = type
        self.unique = unique

    @property
    def disabled(self) -> bool:
        with self.connection.cursor() as c:
            return bool(c.execute("""select is_disabled from sys.objects t
inner join sys.indexes i on t.object_id = %s and t.object_id = i.object_id
where t.is_ms_shipped <> 1 and index_id = %s""" % (self.table.object_id, self.index_id)).fetchall()[0][0])

    def __hash__(self):
        return hash((
            self.table.__hash__(), self.name, tuple(self.columns), self.type, self.unique
        ))

    def __eq__(self, other):
        if isinstance(other, SQLIndex):
            return self.__hash__() == other.__hash__()
        return False

    def __repr__(self):
        return "SQLIndex(table='%s', name='%s', columns=%s, type='%s', unique=%s)" % (
            self.table, self.name, self.columns, self.type, self.unique
        )

    def __str__(self):
        return self.name

    @property
    def connection(self):
        return self.table.connection

    def drop(self):
        with self.connection.cursor() as c:
            c.execute("DROP INDEX %s.[%s]" % (self.table.full_name, self.name))
