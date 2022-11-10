__all__ = [
    "Cursor",
    "prepare_insert_array",
    "prepare_insert_batch",
    "prepare_bulk_csv_array",
    "prepare_bulk_csv_batch"
]


import os
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Generator, Any, Union

import pyarrow.csv as pcsv
import pyarrow.compute as pc
from pyarrow import Schema, schema, field, RecordBatch, Table, RecordBatchReader, \
    Array, ChunkedArray, array, TimestampType, Decimal128Type, Decimal256Type, FixedSizeBinaryType, NativeFile
from pyarrow.fs import FileSystem, LocalFileSystem, FileSelector, FileType, FileInfo
try:
    from pyarrow.parquet import ParquetFile
except ImportError:
    class ParquetFile:
        def __enter__(self):
            pass

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

from .config import DEFAULT_BATCH_ROW_SIZE, DEFAULT_SAFE_MODE, DEFAULT_ARROW_BATCH_ROW_SIZE
from .utils import prepare_insert_batch_statement, prepare_insert_statement
from .utils.typing import ArrowData
from .utils.arrow import cast_arrow, FLOAT64, LARGE_BINARY, BINARY, LARGE_STRING, STRING, TIMES, \
    TIMEUS, TIMEMS, TIMENS, intersect_schemas


def binary_to_hex(scalar) -> Optional[bytes]:
    pyscalar: bytes = scalar.as_py()
    return pyscalar if pyscalar is None else pyscalar.hex()


INSERT_BATCH = {
    TimestampType: lambda arr: pc.utf8_slice_codeunits(arr.cast(STRING), 0, 27),
    TIMES: lambda arr: arr.cast(STRING),
    TIMEMS: lambda arr: arr.cast(STRING),
    TIMEUS: lambda arr: arr.cast(STRING),
    TIMENS: lambda arr: pc.utf8_slice_codeunits(arr.cast(STRING), 0, 16)
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

    def bulk_insert_file(
        self,
        table: Union[str, "SQLTable"],
        file: Union[str, Path],
        commit: bool = True,
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

        :param table: name or SQLTable
        :param file:
        :param commit:
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
        options = "FORMAT = '%s', DATAFILETYPE = '%s',  FIRSTROW = %s, FIELDQUOTE = '%s', ROWTERMINATOR = '%s', " \
                  "CODEPAGE = '%s', FIELDTERMINATOR = '%s'" % (
                      file_format, datafile_type, first_row, field_quote, row_terminator, code_page,
                      field_terminator
                  )
        if tablock:
            options += ", TABLOCK"
        if other_options:
            options += ", " + other_options

        self.execute("""BULK INSERT %s FROM '%s' WITH (%s)""" % (table, file, options))
        if commit:
            self.commit()

    def bulk_insert_arrow(
        self,
        table: Union[str, "SQLTable"],
        data: ArrowData,
        base_dir: Optional[str] = None,
        cast: bool = True,
        safe: bool = DEFAULT_SAFE_MODE,
        commit: bool = True,
        delimiter: str = ",",
        file_format: str = "CSV",
        tablock: bool = False,
        **bulk_options: dict[str, str]
    ):
        if base_dir is None:
            with tempfile.TemporaryDirectory() as base_dir:
                return self.bulk_insert_arrow(
                    table,
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
            if cast:
                data = cast_arrow(data, table.schema_arrow, safe, fill_empty=True, drop=False)

            if isinstance(data, (RecordBatch, Table)):
                tmp_file = os.path.join(base_dir, os.urandom(8).hex()) + ".csv"

                # write in tmp file
                try:
                    pcsv.write_csv(
                        prepare_bulk_csv_batch(data),
                        tmp_file,
                        pcsv.WriteOptions(delimiter=delimiter)
                    )

                    self.bulk_insert_file(
                        table,
                        file=tmp_file,
                        commit=commit,
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
                        table,
                        batch,
                        base_dir=base_dir,
                        cast=cast,
                        safe=safe,
                        commit=commit,
                        delimiter=delimiter,
                        file_format=file_format,
                        tablock=tablock,
                        **bulk_options
                    )

    def insert_arrow(
        self,
        table: Union[str, "SQLTable"],
        data: ArrowData,
        cast: bool = True,
        safe: bool = DEFAULT_SAFE_MODE,
        stmt: str = "",
        commit: bool = True,
        bulk: bool = False,
        tablock: bool = False,
        commit_size: int = 1,
        **insert_options: dict[str, Union[str, int, bool]]
    ):
        if bulk:
            return self.bulk_insert_arrow(
                table=table,
                data=data,
                cast=cast,
                safe=safe,
                tablock=tablock,
                **insert_options
            )
        if cast:
            data = cast_arrow(data, table.schema_arrow, safe, fill_empty=False, drop=True)

        if isinstance(data, (RecordBatch, Table)):
            return self.insert_pylist(
                table,
                [tuple(row.values()) for row in prepare_insert_batch(data).to_pylist()],
                data.schema.names,
                stmt=stmt,
                commit=commit,
                tablock=tablock,
                commit_size=commit_size
            )
        elif isinstance(data, RecordBatchReader):
            commit_size = self.safe_commit_size(commit_size, len(data.schema.names))
            stmt = prepare_insert_batch_statement(
                table, data.schema.names, tablock=tablock, commit_size=commit_size
            )

            for batch in data:
                self.insert_pylist(
                    table,
                    [tuple(row.values()) for row in prepare_insert_batch(batch).to_pylist()],
                    batch.schema.names,
                    stmt=stmt,
                    commit=commit,
                    tablock=tablock,
                    commit_size=commit_size
                )
        else:
            for batch in data:
                self.insert_pylist(
                    table,
                    [tuple(row.values()) for row in prepare_insert_batch(batch).to_pylist()],
                    batch.schema.names,
                    stmt=stmt,
                    commit=commit,
                    tablock=tablock,
                    commit_size=commit_size
                )

    # Parquet insert
    def insert_parquet_file(
        self,
        table: Union[str, "SQLTable"],
        source: Union[ParquetFile, str, Path, NativeFile],
        batch_size: int = DEFAULT_ARROW_BATCH_ROW_SIZE,
        buffer_size: int = 0,
        cast: bool = True,
        safe: bool = DEFAULT_SAFE_MODE,
        commit: bool = True,
        filesystem: FileSystem = LocalFileSystem(),
        coerce_int96_timestamp_unit: str = None,
        use_threads: bool = True,
        exclude_columns: list[str] = (),
        **insert_arrow: dict
    ):
        """
        Insert data from parquet file with pyarrow methods

        :param table: name or SQLTable
        :param source: pyarrow.parquet.ParquetFile, or something to build it
            See https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html#pyarrow.parquet.ParquetFile
        :param batch_size:
        :param buffer_size:
        :param cast:
        :param safe:
        :param commit:
        :param filesystem:
        :param coerce_int96_timestamp_unit:
        :param use_threads:
        :param exclude_columns: list of column names to exclude from parquet read
        :param insert_arrow: self.insert_arrow options
        :return: insert_arrow rtype
        """
        if isinstance(source, ParquetFile):
            input_schema = intersect_schemas(
                schema(
                    [f for f in source.schema_arrow if f.name not in exclude_columns], source.schema_arrow.metadata
                ) if exclude_columns else source.schema_arrow,
                table.schema_arrow.names
            )

            return self.insert_arrow(
                table=table,
                data=RecordBatchReader.from_batches(
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
                **insert_arrow
            )
        elif isinstance(source, str):
            with filesystem.open_input_file(source) as f:
                return self.insert_parquet_file(
                    table=table,
                    source=ParquetFile(
                        f,
                        buffer_size=buffer_size,
                        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit
                    ),
                    batch_size=batch_size,
                    buffer_size=buffer_size,
                    cast=cast,
                    safe=safe,
                    commit=commit,
                    filesystem=filesystem,
                    use_threads=use_threads,
                    exclude_columns=exclude_columns,
                    **insert_arrow
                )
        else:
            return self.insert_parquet_file(
                table=table,
                source=ParquetFile(
                    source,
                    buffer_size=buffer_size,
                    coerce_int96_timestamp_unit=coerce_int96_timestamp_unit
                ),
                batch_size=batch_size,
                buffer_size=buffer_size,
                cast=cast,
                safe=safe,
                commit=commit,
                filesystem=filesystem,
                use_threads=use_threads,
                exclude_columns=exclude_columns,
                **insert_arrow
            )

    def insert_parquet_dir(
        self,
        table: Union[str, "SQLTable"],
        base_dir: str,
        recursive: bool = False,
        allow_not_found: bool = False,
        batch_size: int = DEFAULT_ARROW_BATCH_ROW_SIZE,
        buffer_size: int = 0,
        cast: bool = True,
        safe: bool = DEFAULT_SAFE_MODE,
        commit: bool = True,
        filesystem: FileSystem = LocalFileSystem(),
        coerce_int96_timestamp_unit: str = None,
        use_threads: bool = True,
        exclude_columns: list[str] = (),
        **insert_parquet_file_options: dict
    ) -> Generator[FileInfo, Any, None]:
        """
        Insert data from parquet iterating over files, sub dir files with pyarrow methods

        :param table: name or SQLTable
        :param base_dir:
        :param recursive: recursive fetch files, set True to fetch all files in sub dirs
        :param allow_not_found:
        :param batch_size: number of rows for batch
        :param buffer_size:
        :param cast:
        :param safe:
        :param commit:
        :param filesystem: pyarrow FileSystem object
        :param coerce_int96_timestamp_unit:
        :param use_threads:
        :param exclude_columns: list of column names to exclude from parquet read
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
                        table=table,
                        source=ofs.path,
                        batch_size=batch_size,
                        buffer_size=buffer_size,
                        cast=cast,
                        safe=safe,
                        commit=commit,
                        filesystem=filesystem,
                        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
                        use_threads=use_threads,
                        exclude_columns=exclude_columns,
                        **insert_parquet_file_options
                    )
                    yield ofs
            elif ofs.type == FileType.Directory:
                for _ in self.insert_parquet_dir(
                    table=table,
                    base_dir=ofs.path,
                    batch_size=batch_size,
                    buffer_size=buffer_size,
                    cast=cast,
                    safe=safe,
                    commit=commit,
                    filesystem=filesystem,
                    coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
                    use_threads=use_threads,
                    exclude_columns=exclude_columns,
                    **insert_parquet_file_options
                ):
                    yield _

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
