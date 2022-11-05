import datetime
import decimal

import pyarrow as pa

from msa.utils import pyodbc_description_to_pyarrow_field
from tests import MSSQLTestCase


class PyODBBCUtilsTests(MSSQLTestCase):

    def test_pyodbc_description_to_pyarrow_field(self):
        expected = [
            pa.field("int", pa.int32(), True),
            pa.field("bigint", pa.int64(), True),
            pa.field("bit", pa.bool_(), True),
            pa.field("decimal", pa.decimal128(38, 18), True),
            pa.field("bigdecimal", pa.decimal256(39, 18), True),
            pa.field("float", pa.float64(), True),
            pa.field("real", pa.float32(), True),
            pa.field("date", pa.date32(), True),
            pa.field("datetime", pa.timestamp("ms"), True),
            pa.field("datetime2", pa.timestamp("ns"), True),
            pa.field("smalldatetime", pa.timestamp("s"), True),
            pa.field("time", pa.time64("ns"), True),
            pa.field("string", pa.string(), False),
            pa.field("binary", pa.binary(), True)
        ]

        result = [
            ('int', int, None, 10, 10, 0, True),
            ('bigint', int, None, 19, 19, 0, True),
            ('bit', bool, None, 1, 1, 0, True),
            ('decimal', decimal.Decimal, None, 18, 38, 18, True),
            ('bigdecimal', decimal.Decimal, None, 18, 39, 18, True),
            ('float', float, None, 53, 53, 0, True),
            ('real', float, None, 24, 24, 0, True),
            ('date', datetime.date, None, 10, 10, 0, True),
            ('datetime', datetime.datetime, None, 23, 23, 3, True),
            ('datetime2', datetime.datetime, None, 27, 27, 7, True),
            ('smalldatetime', datetime.datetime, None, 16, 16, 0, True),
            ('time', datetime.time, None, 16, 16, 7, True),
            ('string', str, None, 64, 64, 0, False),
            ('binary', bytearray, None, 64, 64, 0, True),
        ]

        for _1, _2 in zip(result, expected):
            self.assertEqual(pyodbc_description_to_pyarrow_field(_1), _2)
