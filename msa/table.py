__all__ = ["SQLTable"]

import os
import pathlib
import tempfile
from pathlib import Path
from typing import Optional, Any, Union, Iterable, Generator

from pyarrow import Schema, schema, Field, RecordBatch, Table, RecordBatchReader, Decimal128Type, Decimal256Type, field, \
    Array, ChunkedArray, array, TimestampType, Time64Type, Time32Type, FixedSizeBinaryType, NativeFile
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


TABLE_TYPE_COLUMNS_STATEMENT = {
    "BASE TABLE": """select col.name as name,
    type_name(user_type_id) as dtype, col.max_length as max_length, col.precision as precision, col.scale as scale,
    col.is_nullable as nullable, col.collation_name as collation
from sys.tables as tab
inner join sys.columns as col on tab.object_id = col.object_id
where schema_name(tab.schema_id) = '%s' and tab.name = '%s'""",
    "VIEW": """select c.name as name,
    type_name(user_type_id) as dtype, c.max_length, c.precision, c.scale, c.is_nullable as nullable,
    c.collation_name as collation
from sys.columns c
join sys.views v on v.object_id = c.object_id
where schema_name(v.schema_id) = '%s' and object_name(c.object_id) = '%s'"""
}


class SQLTable:

    def __init__(
        self,
        connection: "msa.Connection",
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
        return self.full_name

    @property
    def full_name(self):
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
                    for row in self.connection.cursor().execute(
                        TABLE_TYPE_COLUMNS_STATEMENT[self.type] % (self.schema, self.name)
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

    def prepare_insert_statement(self, columns):
        return "INSERT INTO %s ([%s]) VALUES (%s)" % (
            self.full_name, "],[".join(columns), ",".join(("?" for _ in range(len(columns))))
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
            if cursor is None:
                with self.connection.cursor() as c:
                    return self.insert_pylist(
                        rows,
                        columns,
                        fast_executemany,
                        stmt,
                        commit=commit,
                        cursor=c
                    )
            else:
                try:
                    cursor.fast_executemany = fast_executemany
                except AttributeError:
                    pass

                cursor.executemany(stmt if stmt else self.prepare_insert_statement(columns), rows)

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
        cursor: Optional[Cursor] = None
    ):
        if bulk:
            return self.bulk_insert_arrow(
                data, None, cast, safe,
                cursor=cursor
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
                    cursor=cursor
                )
        else:
            if isinstance(data, (RecordBatch, Table)):
                return self.insert_pylist(
                    [tuple(row.values()) for row in prepare_insert_batch(data).to_pylist()],
                    data.schema.names,
                    fast_executemany=fast_executemany,
                    stmt=stmt,
                    commit=commit,
                    cursor=cursor
                )
            elif isinstance(data, RecordBatchReader):
                stmt = self.prepare_insert_statement(data.schema.names)
                for batch in data:
                    self.insert_pylist(
                        [tuple(row.values()) for row in prepare_insert_batch(batch).to_pylist()],
                        batch.schema.names,
                        fast_executemany=fast_executemany,
                        stmt=stmt,
                        commit=commit,
                        cursor=cursor
                    )
            else:
                for batch in data:
                    self.insert_pylist(
                        [tuple(row.values()) for row in prepare_insert_batch(batch).to_pylist()],
                        batch.schema.names,
                        fast_executemany=fast_executemany,
                        stmt=self.prepare_insert_statement(batch.schema.names),
                        commit=commit,
                        cursor=cursor
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
                    other_options=other_options
                )
        else:
            options = "FORMAT = '%s', DATAFILETYPE = '%s',  FIRSTROW = %s, FIELDQUOTE = '%s', ROWTERMINATOR = '%s', " \
                      "CODEPAGE = '%s'" % (
                          file_format, datafile_type, first_row, field_quote, row_terminator, code_page
                      )
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
        **bulk_options: dict[str, str]
    ):
        if base_dir is None:
            with tempfile.TemporaryDirectory() as base_dir:
                return self.bulk_insert_arrow(
                    data, base_dir=base_dir, cast=cast, safe=safe, commit=commit, cursor=cursor,
                    **bulk_options
                )
        elif cursor is None:
            with self.connection.cursor() as cursor:
                return self.bulk_insert_arrow(
                    data, base_dir=base_dir, cast=cast, safe=safe, commit=commit, cursor=cursor,
                    **bulk_options
                )
        else:
            if cast:
                data = cast_arrow(data, self.schema_arrow, safe, fill_empty=True, drop=False)

            if isinstance(data, (RecordBatch, Table)):
                tmp_file = os.path.join(base_dir, os.urandom(8).hex()) + ".csv"

                # write in tmp file
                try:
                    pcsv.write_csv(prepare_bulk_csv_batch(data), tmp_file)
                    self.bulk_insert_file(
                        file=tmp_file,
                        commit=commit,
                        cursor=cursor,
                        file_format="CSV",
                        field_terminator=",",
                        row_terminator="\n",
                        field_quote='"',
                        datafile_type="char",
                        first_row=2
                    )
                except BaseException as e:
                    raise e
                finally:
                    os.remove(tmp_file)
            else:
                # iterate on record batches
                for batch in data:
                    self.bulk_insert_arrow(
                        batch, base_dir=base_dir, cast=cast, safe=safe, commit=commit, cursor=cursor,
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
        **insert_options: dict
    ):
        if isinstance(source, ParquetFile):
            input_schema = intersect_schemas(source.schema_arrow, self.schema_arrow.names)

            return self.insert_arrow(
                cast_arrow(
                    RecordBatchReader.from_batches(
                        input_schema,
                        source.iter_batches(
                            batch_size=batch_size,
                            columns=input_schema.names,
                            use_threads=use_threads
                        )
                    ),
                    input_schema,
                    safe=safe,
                    fill_empty=False,
                    drop=True
                ),
                cast=False,
                safe=safe,
                commit=commit,
                cursor=cursor,
                **insert_options
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
                    **insert_options
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
                **insert_options
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
