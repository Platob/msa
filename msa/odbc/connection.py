__all__ = ["PyODBCConnection"]

import struct
from datetime import datetime, timedelta, timezone

import numpy
import pymssql
from pyodbc import SQL_TYPE_TIMESTAMP

from msa import Connection as Abstract
from .cursor import PyODBCCursor


def handle_datetime_offset(dto_value):
    # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
    tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 500000000, -6, 0)
    return numpy.datetime64(
        int(datetime(
            tup[0], tup[1], tup[2], tup[3], tup[4], tup[5], 0,
            timezone(timedelta(hours=tup[7], minutes=tup[8]))
        ).timestamp()) * 10 ** 9 + tup[6],
        "ns"
    )


def handle_datetime(dto_value, d1900_1_1=datetime(1900, 1, 1)):
    try:
        tup = struct.unpack("<6hI", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 500000000)
        return numpy.datetime64(
            int(datetime(
                tup[0], tup[1], tup[2], tup[3], tup[4], tup[5], 0,
                timezone(timedelta(hours=0, minutes=0))
            ).timestamp()) * 10 ** 9 + tup[6],
            "ns"
        )
    except struct.error:
        tup = struct.unpack("<2l", dto_value)
        return d1900_1_1 + timedelta(days=tup[0]) + timedelta(seconds=round(tup[1] / 300.0, 3))


class PyODBCConnection(Abstract):

    def __init__(self, server: "PyMSSQL", raw: pymssql.Connection, timeout: int = 0):
        super().__init__(server)
        self.raw = raw

        self.raw.add_output_converter(-155, handle_datetime_offset)
        self.raw.add_output_converter(SQL_TYPE_TIMESTAMP, handle_datetime)
        self.raw.timeout = timeout

    def close(self):
        if not self.closed:
            self.raw.close()
        super(PyODBCConnection, self).close()

    def commit(self) -> None:
        self.raw.commit()

    def rollback(self) -> None:
        self.raw.rollback()

    def cursor(self, fast_executemany: bool = True, *args, **kwargs) -> PyODBCCursor:
        # reconnect if closed
        return self.server.connect().cursor(
            fast_executemany=fast_executemany,
            *args, **kwargs
        ) if self.closed else PyODBCCursor(
            self,
            self.raw.cursor(*args, **kwargs),
            fast_executemany=fast_executemany
        )

    @property
    def timeout(self) -> int:
        return self.raw.timeout

    @timeout.setter
    def timeout(self, timeout: int):
        self.raw.timeout = timeout
