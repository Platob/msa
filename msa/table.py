__all__ = ["SQLTable", "SQLView", "SQLIndex"]

import os
import pathlib
import tempfile
from pathlib import Path
from typing import Optional, Any, Union, Iterable, Generator

from pyarrow import Schema, schema, Field, field, RecordBatch, Table, RecordBatchReader, \
    Array, ChunkedArray, array, TimestampType, Time64Type, Decimal128Type, Decimal256Type, Time32Type, \
    FixedSizeBinaryType, NativeFile
from pyarrow.fs import FileSystem, LocalFileSystem, FileSelector, FileType, FileInfo

import pyarrow.csv as pcsv

try:
    from pyarrow.parquet import ParquetFile
except ImportError:
    class ParquetFile:
        def __enter__(self):
            pass

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

from .config import DEFAULT_SAFE_MODE
from .cursor import Cursor
from .utils import mssql_column_to_pyarrow_field
from .utils.arrow import cast_arrow, FLOAT64, LARGE_BINARY, BINARY, LARGE_STRING, STRING, intersect_schemas


def binary_to_hex(scalar) -> Optional[bytes]:
    pyscalar: bytes = scalar.as_py()
    return pyscalar if pyscalar is None else pyscalar.hex()


INSERT_BATCH = {
    TimestampType: lambda arr: arr.cast(STRING),
    Time32Type: lambda arr: arr.cast(STRING),
    Time64Type: lambda arr: arr.cast(STRING)
}

CSV_DTYPES = {
    Decimal128Type: lambda arr: arr.cast(FLOAT64),
    Decimal256Type: lambda arr: arr.cast(FLOAT64),
    BINARY: lambda arr: array([binary_to_hex(_) for _ in arr], STRING),
    FixedSizeBinaryType: lambda arr: array([binary_to_hex(_) for _ in arr], STRING),
    LARGE_BINARY: lambda arr: array([binary_to_hex(_) for _ in arr], LARGE_STRING),
    **INSERT_BATCH
}


def prepare_bulk_csv_array(arr: Union[Array, ChunkedArray]):
    if arr.type in CSV_DTYPES:
        return CSV_DTYPES[arr.type](arr)
    elif arr.type.__class__ in CSV_DTYPES:
        return CSV_DTYPES[arr.type.__class__](arr)
    return arr


def prepare_bulk_csv_batch(data: Union[RecordBatch, Table]) -> Union[RecordBatch, Table]:
    arrays = [prepare_bulk_csv_array(arr) for arr in data]

    return data.__class__.from_arrays(arrays, schema=schema([
        field(data.schema[i].name, arrays[i].type, data.schema[i].nullable)
        for i in range(len(data.schema))
    ]))


def prepare_insert_array(arr: Union[Array, ChunkedArray]):
    if arr.type in INSERT_BATCH:
        return INSERT_BATCH[arr.type](arr)
    elif arr.type.__class__ in INSERT_BATCH:
        return INSERT_BATCH[arr.type.__class__](arr)
    return arr


def prepare_insert_batch(data: Union[RecordBatch, Table]) -> Union[RecordBatch, Table]:
    arrays = [prepare_insert_array(arr) for arr in data]

    return data.__class__.from_arrays(arrays, schema=schema([
        field(data.schema[i].name, arrays[i].type, data.schema[i].nullable)
        for i in range(len(data.schema))
    ]))


def linearize(it):
    for _0 in it:
        for _1 in _0:
            yield _1


def concat_rows(rows, batch_size: int = 1):
    while len(rows) > batch_size:
        resized, rows = rows[:batch_size], rows[batch_size:]
        yield len(resized), list(linearize(resized))
    if len(rows) > 0:
        yield len(rows), list(linearize(rows))


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
        if self.__connection.closed:
            self.__connection = self.__connection.server.connect()
        return self.__connection

    @connection.setter
    def connection(self, connection):
        from .connection import Connection
        self.__connection: Connection = connection

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

    def prepare_insert_statement(
        self,
        columns: list[str],
        tablock: bool = False
    ):
        return "INSERT INTO %s%s([%s]) VALUES (%s)" % (
            self.full_name,
            "WITH(TABLOCK)" if tablock else "",
            "],[".join(columns),
            ",".join(("?" for _ in range(len(columns))))
        )

    def prepare_insert_batch_statement(
        self,
        columns: list[str],
        tablock: bool = False,
        commit_size: int = 1
    ):
        values = "(%s)" % ",".join(("?" for _ in range(len(columns))))
        return "INSERT INTO %s%s([%s]) VALUES %s" % (
            self.full_name,
            "WITH(TABLOCK)" if tablock else "",
            "],[".join(columns),
            ",".join((values for _ in range(commit_size)))
        )

    def insert_pylist(
        self,
        rows: list[Union[list[Any], tuple[Any]]],
        columns: list[str],
        fast_executemany: bool = True,
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
        :param fast_executemany: pyodbc.Cursor.fast_executemany, default = True
        :param stmt:
        :param commit: commit at the end
        :param cursor: existing connection.Cursor, default will create new one
        :param tablock: see self.prepare_insert_statement or self.prepare_insert_batch_statement
        :param commit_size: number of row batch in insert values, must be 0 > batch_size > 1001
        :return: None
        """
        if rows:
            if cursor is None:
                with self.connection.cursor() as c:
                    return self.insert_pylist(
                        rows,
                        columns,
                        fast_executemany,
                        stmt,
                        commit=commit,
                        cursor=c,
                        tablock=tablock,
                        commit_size=commit_size
                    )
            else:
                try:
                    cursor.fast_executemany = fast_executemany
                except AttributeError:
                    pass

                if commit_size > 1:
                    default_stmt = stmt if stmt else self.prepare_insert_batch_statement(
                        columns, tablock=tablock, commit_size=commit_size
                    )

                    for num_rows, row_batch in concat_rows(rows, commit_size):
                        cursor.executemany(
                            default_stmt if num_rows == commit_size else self.prepare_insert_batch_statement(
                                columns, tablock=tablock, commit_size=num_rows
                            ),
                            [row_batch]
                        )
                else:
                    cursor.executemany(
                        stmt if stmt else self.prepare_insert_statement(columns, tablock=tablock),
                        rows
                    )

                if commit:
                    cursor.commit()

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
        bulk: bool = False,
        cursor: Optional[Cursor] = None,
        tablock: bool = False,
        commit_size: int = 1
    ):
        if bulk:
            return self.bulk_insert_arrow(
                data, None, cast, safe,
                cursor=cursor,
                tablock=tablock
            )
        if cast:
            data = cast_arrow(data, self.schema_arrow, safe, fill_empty=False, drop=True)

        if cursor is None:
            with self.connection.cursor() as cursor:
                return self.insert_arrow(
                    data,
                    cast=cast,
                    safe=safe,
                    fast_executemany=fast_executemany,
                    stmt=stmt,
                    commit=commit,
                    bulk=bulk,
                    cursor=cursor,
                    tablock=tablock,
                    commit_size=commit_size
                )
        else:
            if isinstance(data, (RecordBatch, Table)):
                return self.insert_pylist(
                    [tuple(row.values()) for row in prepare_insert_batch(data).to_pylist()],
                    data.schema.names,
                    fast_executemany=fast_executemany,
                    stmt=stmt,
                    commit=commit,
                    cursor=cursor,
                    tablock=tablock,
                    commit_size=commit_size
                )
            elif isinstance(data, RecordBatchReader):
                stmt = self.prepare_insert_batch_statement(data.schema.names, tablock=tablock, commit_size=commit_size)

                for batch in data:
                    self.insert_pylist(
                        [tuple(row.values()) for row in prepare_insert_batch(batch).to_pylist()],
                        batch.schema.names,
                        fast_executemany=fast_executemany,
                        stmt=stmt,
                        commit=commit,
                        cursor=cursor,
                        tablock=tablock,
                        commit_size=commit_size
                    )
            else:
                for batch in data:
                    self.insert_pylist(
                        [tuple(row.values()) for row in prepare_insert_batch(batch).to_pylist()],
                        batch.schema.names,
                        fast_executemany=fast_executemany,
                        stmt=stmt,
                        commit=commit,
                        cursor=cursor,
                        tablock=tablock,
                        commit_size=commit_size
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
            with self.connection.cursor() as cursor:
                return self.bulk_insert_file(
                    file, commit, cursor, file_format,
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
            options = "FORMAT = '%s', DATAFILETYPE = '%s',  FIRSTROW = %s, FIELDQUOTE = '%s', ROWTERMINATOR = '%s', " \
                      "CODEPAGE = '%s', FIELDTERMINATOR = '%s'" % (
                        file_format, datafile_type, first_row, field_quote, row_terminator, code_page,
                        field_terminator
                      )
            if tablock:
                options += ", TABLOCK"
            if other_options:
                options += ", " + other_options

            cursor.execute("""BULK INSERT %s FROM '%s' WITH (%s)""" % (self.full_name, file, options))
            if commit:
                cursor.commit()

    def bulk_insert_arrow(
        self,
        data: Union[
            RecordBatch, Table,
            Iterable[Union[RecordBatch, Table]], RecordBatchReader
        ],
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
        if base_dir is None:
            with tempfile.TemporaryDirectory() as base_dir:
                return self.bulk_insert_arrow(
                    data, base_dir=base_dir, cast=cast, safe=safe, commit=commit, cursor=cursor,
                    delimiter=delimiter,
                    file_format=file_format,
                    tablock=tablock,
                    **bulk_options
                )
        elif cursor is None:
            with self.connection.cursor() as cursor:
                return self.bulk_insert_arrow(
                    data, base_dir=base_dir, cast=cast, safe=safe, commit=commit, cursor=cursor,
                    delimiter=delimiter,
                    file_format=file_format,
                    tablock=tablock,
                    **bulk_options
                )
        else:
            if cast:
                data = cast_arrow(data, self.schema_arrow, safe, fill_empty=True, drop=False)

            if isinstance(data, (RecordBatch, Table)):
                tmp_file = os.path.join(base_dir, os.urandom(8).hex()) + ".csv"

                # write in tmp file
                try:
                    pcsv.write_csv(prepare_bulk_csv_batch(data), tmp_file, pcsv.WriteOptions(delimiter=delimiter))
                    self.bulk_insert_file(
                        file=tmp_file,
                        commit=commit,
                        cursor=cursor,
                        file_format=file_format,
                        field_terminator=delimiter,
                        row_terminator="\n",
                        field_quote='"',
                        datafile_type="char",
                        first_row=2,
                        tablock=tablock,
                        **bulk_options
                    )
                finally:
                    os.remove(tmp_file)
            else:
                # iterate on record batches
                for batch in data:
                    self.bulk_insert_arrow(
                        batch, base_dir=base_dir, cast=cast, safe=safe, commit=commit, cursor=cursor,
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
        batch_size: int = 65536,
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
        if isinstance(source, ParquetFile):
            input_schema = intersect_schemas(source.schema_arrow, self.schema_arrow.names)

            return self.insert_arrow(
                RecordBatchReader.from_batches(
                    input_schema,
                    source.iter_batches(
                        batch_size=batch_size,
                        columns=input_schema.names,
                        use_threads=use_threads
                    )
                ),
                cast=cast,
                safe=safe,
                commit=commit,
                cursor=cursor,
                **insert_arrow
            )
        elif isinstance(source, str):
            with filesystem.open_input_file(source) as f:
                return self.insert_parquet_file(
                    ParquetFile(
                        f,
                        buffer_size=buffer_size,
                        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit
                    ),
                    batch_size=batch_size,
                    buffer_size=buffer_size,
                    cast=cast,
                    safe=safe,
                    commit=commit,
                    cursor=cursor,
                    filesystem=filesystem,
                    use_threads=use_threads,
                    **insert_arrow
                )
        else:
            return self.insert_parquet_file(
                ParquetFile(
                    source,
                    buffer_size=buffer_size,
                    coerce_int96_timestamp_unit=coerce_int96_timestamp_unit
                ),
                batch_size=batch_size,
                buffer_size=buffer_size,
                cast=cast,
                safe=safe,
                commit=commit,
                cursor=cursor,
                filesystem=filesystem,
                use_threads=use_threads,
                **insert_arrow
            )

    def insert_parquet_dir(
        self,
        base_dir: str,
        recursive: bool = False,
        allow_not_found: bool = False,
        batch_size: int = 65536,
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
        :param insert_parquet_file_options: other self.insert_parquet_file options
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
