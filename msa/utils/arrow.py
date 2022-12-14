__all__ = [
    "iter_dir_files",
    "cast_batch", "cast_array", "cast_arrow",
    "intersect_schemas",
    "timestamp_to_timestamp",
    "FLOAT64",
    "LARGE_BINARY", "BINARY",
    "LARGE_STRING", "STRING",
    "TIMES", "TIMEMS", "TIMEUS", "TIMENS"
]

import datetime
import re
from typing import Union, Iterable, Generator, Optional, Callable, Sized

import pyarrow
import pyarrow as pa
import pyarrow.compute as pc

from pyarrow import RecordBatch, Schema, schema as schema_builder, Field, field as field_builder, Array, DataType, \
    Decimal128Type, Decimal256Type, TimestampType, ArrowInvalid, Time64Type, Table, RecordBatchReader, array, Time32Type
from pyarrow.fs import FileSystem, FileInfo, FileType, FileSelector

from ..config import DEFAULT_SAFE_MODE


STRING = UTF8 = pa.string()
LARGE_STRING = pa.large_string()
BOOL = BOOLEAN = pa.bool_()
INT = INT32 = pa.int32()
INT8, INT16, INT64 = pa.int8(), pa.int16(), pa.int64()
UINT = UINT32 = pa.uint32()
UINT8, UINT16, UINT64 = pa.uint8(), pa.uint16(), pa.uint64()
FLOAT16, FLOAT32, FLOAT64 = pa.float16(), pa.float32(), pa.float64()
DOUBLE = FLOAT64
DATE = DATE32 = pa.date32()
DATE64 = pa.date64()
DATETIME = TIMESTAMP = pa.timestamp("ns")
UTCTIMESTAMP = pa.timestamp("ns", "UTC")
TIME = TIME64 = TIMENS = pa.time64("ns")
TIMEUS = pa.time64("us")
TIMEMS = TIME32 = pa.time32("ms")
TIMES = pa.time32("s")
TIMETYPES = {
    "s": TIMES, "ms": TIMEMS, "us": TIMEUS, "ns": TIMENS
}
BINARY = pa.binary(-1)
LARGE_BINARY = pa.large_binary()
NULL = pa.null()


def get_field(
    schema: Schema,
    name: str,
    raise_error: bool = True,
    replace_name: bool = False
) -> Optional[Field]:
    try:
        idx = schema.names.index(name)
        return schema.field(idx)
    except ValueError:
        # b.schema.names.index("name")
        # ValueError: 'name' is not in list
        for batch_field in schema:
            if batch_field.name.lower() == name.lower():
                return field_builder(
                    name, batch_field.type, batch_field.nullable, batch_field.metadata
                ) if replace_name else batch_field
        if raise_error:
            raise KeyError("Cannot find Field<'%s'> in schema %s" % (
                name, schema.names
            ))
        return None


def intersect_schemas(schema: Schema, names: list[str], replace_name: bool = False):
    fields = [get_field(schema, name, False, replace_name) for name in names]
    return schema_builder(
        [_ for _ in fields if _ is not None],
        schema.metadata
    )


def get_batch_column_or_empty(
    batch: Union[RecordBatch, Table], field: Field,
    fill_empty: bool = True,
    drop: bool = False
) -> (Field, Array):
    try:
        idx = batch.schema.names.index(field.name)
        return batch.schema.field(idx), batch.column(idx)
    except ValueError:
        # b.schema.names.index("name")
        # ValueError: 'name' is not in list
        data, idx = None, -1

        for batch_field in batch.schema:
            idx += 1

            if batch_field.name.lower() == field.name.lower():
                data = field_builder(field.name, batch_field.type, batch_field.nullable, batch_field.metadata), batch.column(idx)
                break

        if data:
            return data
        elif field.nullable and fill_empty:
            return field, pa.array([None] * batch.num_rows, field.type)
        elif drop:
            return None
        else:
            raise KeyError("Cannot find Field<'%s', %s> in batch columns %s, or fill with nulls" % (
                field.name, field.type, batch.schema.names
            ))


def string_to_timestamp(arr: Array, dtype: TimestampType, safe: bool = DEFAULT_SAFE_MODE, **kwargs):
    try:
        return arr.cast(dtype, safe)
    except ArrowInvalid as e:
        if safe:
            raise e
        else:
            import pandas

            return timestamp_to_timestamp(
                Array.from_pandas(pandas.to_datetime(arr.to_pandas())),
                dtype,
                safe,
                **kwargs
            )


def string_to_date(arr: Array, safe: bool = DEFAULT_SAFE_MODE, **kwargs):
    if safe:
        return array(
            [None if _ is None else datetime.date.fromisoformat(_) for _ in (_.as_py() for _ in arr)],
            DATE, safe=safe
        )
    return string_to_timestamp(arr, TIMESTAMP, safe=safe).cast(DATE, safe)


def string_to_time(arr: Array, dtype: Time32Type, safe: bool = DEFAULT_SAFE_MODE, **kwargs):
    try:
        unit = dtype.unit
    except AttributeError:
        unit = re.findall(r"\[(.*?)\]", str(dtype))[0]
    return string_to_timestamp(arr, pyarrow.timestamp(unit), safe=safe).cast(dtype, safe)


def timestamp_to_timestamp(arr: Array, dtype: TimestampType, safe: bool = DEFAULT_SAFE_MODE, **kwargs):
    if arr.type.tz is None:
        # naive
        if dtype.tz in {"UTC", "GMT"} or dtype.tz == arr.type.tz:
            return arr.cast(dtype, safe)
        else:
            # need assume timezone
            # return Array.from_pandas(
            #     arr.to_pandas().dt.tz_localize(dtype.tz),
            #     safe=safe
            # ).cast(dtype, safe)
            return pc.assume_timezone(
                arr,
                dtype.tz,
                ambiguous="raise" if safe else "earliest",
                nonexistent="raise" if safe else "earliest"
            ) if arr.type.unit == dtype.unit else pc.assume_timezone(
                arr,
                dtype.tz,
                ambiguous="raise" if safe else "earliest",
                nonexistent="raise" if safe else "earliest"
            ).cast(dtype, safe)
    else:
        return arr.cast(dtype, safe)


TYPE_CASTS = {
    (STRING, INT8): lambda arr, safe=DEFAULT_SAFE_MODE, **kwargs:
        pc.round(arr.cast(FLOAT32, safe=safe)).cast(INT8, safe=safe),
    (STRING, INT16): lambda arr, safe=DEFAULT_SAFE_MODE, **kwargs:
        pc.round(arr.cast(FLOAT32, safe=safe)).cast(INT16, safe=safe),
    (STRING, INT32): lambda arr, safe=DEFAULT_SAFE_MODE, **kwargs:
        pc.round(arr.cast(FLOAT64, safe=safe)).cast(INT32, safe=safe),
    (STRING, INT64): lambda arr, safe=DEFAULT_SAFE_MODE, **kwargs:
        pc.round(arr.cast(FLOAT64, safe=safe)).cast(INT64, safe=safe),
    (STRING, Decimal128Type): lambda arr, dtype, safe=DEFAULT_SAFE_MODE, **kwargs:
        arr.cast(FLOAT64, safe=safe).cast(dtype, safe=safe),
    (STRING, Decimal256Type): lambda arr, dtype, safe=DEFAULT_SAFE_MODE, **kwargs:
        arr.cast(FLOAT64, safe=safe).cast(dtype, safe=safe),
    (STRING, TimestampType): string_to_timestamp,
    (STRING, DATE): string_to_date,
    (STRING, TIMES): string_to_time,
    (STRING, TIMEMS): string_to_time,
    (STRING, TIMEUS): string_to_time,
    (STRING, TIMENS): string_to_time,
    (TimestampType, TimestampType): timestamp_to_timestamp
}


def cast_array(array: Array, dtype: DataType, safe: bool = DEFAULT_SAFE_MODE):
    if array.type.equals(dtype):
        return array
    elif (array.type, dtype) in TYPE_CASTS:
        return TYPE_CASTS[(array.type, dtype)](array, safe=safe, dtype=dtype)
    elif (array.type.__class__, dtype) in TYPE_CASTS:
        return TYPE_CASTS[(array.type.__class__, dtype)](array, safe=safe, dtype=dtype)
    elif (array.type, dtype.__class__) in TYPE_CASTS:
        return TYPE_CASTS[(array.type, dtype.__class__)](array, safe=safe, dtype=dtype)
    elif (array.type.__class__, dtype.__class__) in TYPE_CASTS:
        return TYPE_CASTS[(array.type.__class__, dtype.__class__)](array, safe=safe, dtype=dtype)
    else:
        return array.cast(dtype, safe=safe)


def cast_batch(
    batch: Union[RecordBatch, Table], schema: Schema, safe: bool = DEFAULT_SAFE_MODE,
    fill_empty: bool = True,
    drop: bool = False
) -> Union[RecordBatch, Table]:
    # check names
    if batch.schema.names != schema.names:
        columns: list[(Field, Array)] = [get_batch_column_or_empty(batch, field, fill_empty, drop) for field in schema]

        if drop:
            columns = [c for c in columns if c is not None]
            schema = schema_builder([_[0] for _ in columns], schema.metadata)

        return cast_batch(
            batch.__class__.from_arrays(
                [_[1] for _ in columns],
                schema=schema_builder([_[0] for _ in columns])
            ),
            schema=schema,
            safe=safe,
            fill_empty=fill_empty,
            drop=drop
        )

    if batch.schema == schema:
        return batch.replace_schema_metadata(schema.metadata)
    else:
        # check data types and cast
        return batch.__class__.from_arrays(
            [
                cast_array(batch.column(i), schema.field(i).type, safe=safe)
                for i in range(len(schema))
            ],
            schema=schema
        )


def cast_arrow(
    data: Union[
        RecordBatch, Table,
        Iterable[Union[RecordBatch, Table]],
        RecordBatchReader
    ],
    schema: Schema,
    safe: bool = DEFAULT_SAFE_MODE,
    fill_empty: bool = True,
    drop: bool = False
) -> Union[RecordBatch, RecordBatchReader, Generator[RecordBatch, None, None]]:
    if isinstance(data, RecordBatch):
        return cast_batch(data, schema, safe, fill_empty, drop)
    elif isinstance(data, Table):
        data = cast_batch(data, schema, safe, fill_empty, drop)
        return RecordBatchReader.from_batches(data.schema, data.to_batches())
    elif drop:
        if isinstance(data, Sized):
            if len(data) == 0:
                data = RecordBatchReader.from_batches(schema, [])
            else:
                data = cast_arrow(
                    RecordBatchReader.from_batches(data[0].schema, data),
                    schema,
                    safe,
                    fill_empty,
                    drop
                )
        elif isinstance(data, RecordBatchReader):
            inter = intersect_schemas(schema, data.schema.names)
            return RecordBatchReader.from_batches(
                inter,
                (cast_batch(_, inter, safe, fill_empty, False) for _ in data)
            )
        return (cast_batch(_, schema, safe, fill_empty, drop) for _ in data)
    else:
        return RecordBatchReader.from_batches(schema, (cast_batch(_, schema, safe, fill_empty, False) for _ in data))


# FileSystem
def iter_dir_files(
    fs: FileSystem,
    path: str,
    allow_not_found: bool = False,
) -> Generator[FileInfo, None, None]:
    for ofs in fs.get_file_info(
        FileSelector(path, allow_not_found=allow_not_found, recursive=False)
    ):
        # <FileInfo for 'path': type=FileType.Directory>
        # or <FileInfo for 'path': type=FileType.File, size=0>
        if ofs.is_file:
            yield ofs
        elif ofs.type == FileType.Directory:
            for file in iter_dir_files(fs, ofs.path, allow_not_found):
                yield file
