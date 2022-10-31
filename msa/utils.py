__all__ = [
    "pyodbc_description_to_pyarrow_field"
]

import datetime
import decimal
from typing import Optional

import pyarrow
import pyarrow as pa
from pyarrow import Field, field

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


def int_to_timeunit(i: int) -> str:
    if i == 0:
        return "s"
    elif i <= 3:
        return "ms"
    elif i <= 6:
        return "us"
    return "ns"


def fine_int(precision: int):
    if precision <= 3:
        return INT8
    if precision <= 5:
        return INT16
    if precision <= 10:
        return INT32
    return INT64


def fine_float(precision: int):
    return FLOAT32 if precision < 25 else FLOAT64


def fine_decimal(precision: int, scale: int):
    if precision > 38:
        return pyarrow.decimal256(precision, scale)
    else:
        return pyarrow.decimal128(precision, scale)


DATATYPES = {
    bytes: lambda precision=None, *args, **kwargs:
        LARGE_BINARY if precision is not None and int(precision) > 42000 else BINARY,
    bytearray: lambda precision=None, *args, **kwargs:
        LARGE_BINARY if precision is not None and int(precision) > 42000 else BINARY,
    str: lambda precision=None, *args, **kwargs:
        LARGE_STRING if precision is not None and int(precision) > 42000 else STRING,
    bool: lambda *args, **kwargs: BOOL,
    float: lambda precision, *args, **kwargs: fine_float(precision),
    int: lambda precision, *args, **kwargs: fine_int(precision),
    decimal.Decimal: lambda precision, scale, *args, **kwargs: fine_decimal(precision, scale),
    datetime.date: lambda *args, **kwargs: DATE,
    datetime.time: lambda scale, *args, **kwargs: TIMETYPES[int_to_timeunit(scale)],
    datetime.datetime: lambda scale=None, *args, **kwargs: pa.timestamp(int_to_timeunit(scale)),
}


def pyodbc_description_to_pyarrow_field(description: tuple, metadata: Optional[dict] = None) -> Field:
    """
    Parse input cursor.description
    (name, type_code, display_size, internal_size, precision, scale, null_ok)
    Example: ('string', <class 'str'>, None, 64, 64, 0, False)

    :rtype: Field
    """
    name, type_code, _, _, precision, scale, null_ok = description
    return field(
        name,
        DATATYPES[type_code](precision=precision, scale=scale),
        nullable=null_ok,
        metadata=metadata
    )
