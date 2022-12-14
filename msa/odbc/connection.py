__all__ = ["PyODBCConnection"]

import struct
from datetime import datetime, timedelta, timezone

import numpy
import pyodbc
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
        # e.g., (2017, 3, 16, 10, 35, 18, 500000000)
        year, month, day, hour, minute, second, ns = struct.unpack("<6hI", dto_value)
        return numpy.datetime64(
            f"{year:02}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{ns:09}",
            "ns"
        )
    except struct.error:
        tup = struct.unpack("<2l", dto_value)
        return d1900_1_1 + timedelta(days=tup[0]) + timedelta(seconds=round(tup[1] / 300.0, 3))


class PyODBCConnection(Abstract):

    def __init__(self, server: "PyODBC", raw: pyodbc.Connection, timeout: int = 0):
        super().__init__(server)
        self.raw = raw

        self.raw.add_output_converter(-155, handle_datetime_offset)
        self.raw.add_output_converter(SQL_TYPE_TIMESTAMP, handle_datetime)
        self.timeout = timeout

    def reconnect(self):
        self.raw = self.server.connect(timeout=self.timeout).raw

        self.raw.add_output_converter(-155, handle_datetime_offset)
        self.raw.add_output_converter(SQL_TYPE_TIMESTAMP, handle_datetime)

        self.closed = False

    def close(self):
        if not self.closed:
            self.raw.close()
            del self.raw
            self.raw = None
        super(PyODBCConnection, self).close()

    def commit(self) -> None:
        self.raw.commit()

    def rollback(self) -> None:
        self.raw.rollback()

    def cursor(self, fast_executemany: bool = True, nocount: bool = True) -> PyODBCCursor:
        # reconnect if closed
        return self.server.connect(timeout=self.timeout).cursor(
            fast_executemany=fast_executemany,
            nocount=nocount
        ) if self.closed else PyODBCCursor(
            self,
            self.raw.cursor(),
            fast_executemany=fast_executemany,
            nocount=nocount
        )

    @property
    def timeout(self) -> int:
        return self.__timeout

    @timeout.setter
    def timeout(self, timeout: int):
        self.raw.timeout = self.__timeout = timeout
