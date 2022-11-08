import pyarrow as pa

from msa.utils import mssql_column_to_pyarrow_field
from tests import MSSQLTestCase


class UtilsTests(MSSQLTestCase):

    def test_pyodbc_description_to_pyarrow_field(self):
        expected = [
            pa.field("int", pa.int32(), True),
            pa.field("bigint", pa.int64(), True),
            pa.field("bit", pa.bool_(), True),
            pa.field("decimal", pa.decimal128(38, 38), True),
            pa.field("bigdecimal", pa.decimal256(39, 38), True),
            pa.field("float", pa.float64(), True),
            pa.field("real", pa.float32(), True),
            pa.field("date", pa.date32(), True),
            pa.field("datetime", pa.timestamp("ms"), True),
            pa.field("datetime2", pa.timestamp("ns"), True),
            pa.field("smalldatetime", pa.timestamp("s"), True),
            pa.field("time", pa.time64("ns"), True),
            pa.field("varchar", pa.large_string(), False),
            pa.field("nvarchar", pa.large_string(), False),
            pa.field("binary", pa.large_binary(), True),
            pa.field("uniqueidentifier", pa.string(), True),
            pa.field("datetime_offset", pa.timestamp("ns", "UTC"), True),
            pa.field("text", pa.large_string(), True)
        ]

        result = [
            ('int', 'int', 4, 10, 0, True, None, False),
            ('bigint', 'bigint', 8, 19, 0, True, None, False),
            ('bit', 'bit', 1, 1, 0, True, None, False),
            ('decimal', 'decimal', 17, 38, 38, True, None, False),
            ('bigdecimal', 'decimal', 17, 39, 38, True, None, False),
            ('float', 'float', 8, 53, 0, True, None, False),
            ('real', 'real', 4, 24, 0, True, None, False),
            ('date', 'date', 3, 10, 0, True, None, False),
            ('datetime', 'datetime', 8, 23, 3, True, None, False),
            ('datetime2', 'datetime2', 8, 27, 7, True, None, False),
            ('smalldatetime', 'smalldatetime', 4, 16, 0, True, None, False),
            ('time', 'time', 5, 16, 7, True, None, False),
            ('varchar', 'varchar', 8000, 0, 0, False, 'SQL_Latin1_General_CP1_CI_AS', False),
            ('nvarchar', 'nvarchar', 8000, 0, 0, False, 'SQL_Latin1_General_CP1_CI_AS', False),
            ('binary', 'varbinary', 8000, 0, 0, True, None, False),
            ('uniqueidentifier', 'uniqueidentifier', 16, 0, 0, True, None, False),
            ('datetime_offset', 'datetimeoffset', 10, 34, 7, True, None, False),
            ('text', 'text', 16, 0, 0, True, 'SQL_Latin1_General_CP1_CI_AS', False)
        ]

        for _1, _2 in zip(result, expected):
            self.assertEqual(mssql_column_to_pyarrow_field(_1), _2)

    def test_mssql_column_to_pyarrow_field_metadata(self):
        self.assertEqual(
            {b'collation': b'SQL_Latin1_General_CP1_CI_AS', b'identity': b'true', b'precision': b'10', b'scale': b'0'},
            mssql_column_to_pyarrow_field(('id', 'int', 4, 10, 0, True, "SQL_Latin1_General_CP1_CI_AS", True)).metadata
        )
