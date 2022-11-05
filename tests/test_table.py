import datetime

import numpy
import pyarrow
import pyarrow as pa
from pyarrow import schema

from tests import MSSQLTestCase


class TableTests(MSSQLTestCase):
    server = MSSQLTestCase.get_test_pyodbc_server()

    def setUp(self) -> None:
        MSSQLTestCase.create_test_table(self.server)
        self.table = self.server.connect().table(MSSQLTestCase.PYMSA_UNITTEST)

    def tearDown(self) -> None:
        self.table.drop()

    def test_table_schema_arrow(self):
        expected = schema([
            pa.field("int", pa.int32(), True),
            pa.field("bigint", pa.int64(), True),
            pa.field("bit", pa.bool_(), True),
            pa.field("decimal", pa.decimal128(38, 38), True),
            pa.field("float", pa.float64(), True),
            pa.field("real", pa.float32(), True),
            pa.field("date", pa.date32(), True),
            pa.field("datetime", pa.timestamp("ms"), True),
            pa.field("datetime2", pa.timestamp("ns"), True),
            pa.field("smalldatetime", pa.timestamp("s"), True),
            pa.field("time", pa.time64("ns"), True),
            pa.field("string", pa.large_string(), True),
            pa.field("binary", pa.binary(), True),
            pa.field("uniqueidentifier", pa.string(), True),
            pa.field("datetime_offset", pa.timestamp("ns", "UTC"), True)
        ])

        self.assertEqual(self.table.schema_arrow, expected)

    def test_prepare_insert_statement(self):
        self.assertEqual(
            "INSERT INTO [master].[dbo].[PYMSA_UNITTEST] ([a]) VALUES (?)",
            self.table.prepare_insert_statement(["a"])
        )

    def test_repr(self):
        self.assertEqual(
            "SQLTable('master', 'dbo', 'PYMSA_UNITTEST')",
            repr(self.table)
        )

    def test_str(self):
        self.assertEqual(
            "[master].[dbo].[PYMSA_UNITTEST]",
            str(self.table)
        )

    def test_get_field(self):
        self.assertEqual(
            self.table.field("string"),
            pa.field("string", pa.large_string(), True)
        )

    def test_get_field_case_insensitive(self):
        self.assertEqual(
            self.table.field("String"),
            pa.field("string", pa.large_string(), True)
        )
        self.assertEqual(
            self.table["String"],
            pa.field("string", pa.large_string(), True)
        )

    def test_insert_pylist(self):
        self.table.insert_pylist([[1, None]], ["string", "int"])
        self.assertEqual(
            [["1", None]],
            [
                list(_)
                for _ in self.server.cursor().execute(f"select string, int from {self.PYMSA_UNITTEST}").fetchall()
            ]
        )

    def test_insert_large_pylist(self):
        data = [
            ["1", None] for _ in range(1001)
        ]
        self.table.insert_pylist(data, ["string", "int"])
        self.assertEqual(
            data,
            [
                list(_)
                for _ in self.server.cursor().execute(f"select string, int from {self.PYMSA_UNITTEST}").fetchall()
            ]
        )

    def test_insert_pylist_empty(self):
        self.table.insert_pylist([], ["string", "int"])
        self.assertEqual(
            [],
            [
                list(_)
                for _ in self.server.cursor().execute(f"select string, int from {self.PYMSA_UNITTEST}").fetchall()
            ]
        )

    def test_insert_pyarrow_batch(self):
        data = pyarrow.RecordBatch.from_arrays([
            pyarrow.array([1, None]),
            pyarrow.array(['test', 'test']),
            pyarrow.array([datetime.date(2022, 10, 20), None]),
            pyarrow.array([None, None]),
            pyarrow.array([None, None]),
            pyarrow.array([datetime.datetime(2017, 3, 16, 10, 35, 18, 123000), None])
            .cast(pyarrow.timestamp("ms"), False),
            pyarrow.array([numpy.datetime64('2017-03-16T10:35:18.123456800'), None]),
            pyarrow.array([b"test", None])
        ], schema=pyarrow.schema([
            pyarrow.field("int", pyarrow.int32(), nullable=True),
            pyarrow.field("string", pyarrow.string(), nullable=False),
            pyarrow.field("date", pyarrow.date32(), nullable=True),
            pyarrow.field("float", pyarrow.float64(), nullable=True),
            pyarrow.field("real", pyarrow.float32(), nullable=True),
            pyarrow.field("datetime", pyarrow.timestamp("ms"), nullable=True),
            pyarrow.field("datetime2", pyarrow.timestamp("ns"), nullable=True),
            pyarrow.field("binary", pyarrow.binary(), nullable=True)
        ]))

        self.table.truncate()
        self.table.insert_arrow(data, cast=False, safe=True, commit=True)

        result = [
            list(_)
            for _ in self.server.cursor().execute(
                f"select int, string, date, float, real, datetime, datetime2, binary from {self.PYMSA_UNITTEST}"
            ).fetchall()
        ]

        self.assertEqual(
            [
                [
                    1, 'test', datetime.date(2022, 10, 20), None, None,
                    datetime.datetime(2017, 3, 16, 10, 35, 18, 123000),
                    numpy.datetime64('2017-03-16T10:35:18.123456000'), b'test'
                ],
                [
                    None, 'test', None, None, None, None, None, None
                ]
            ],
            result
        )

    def test_truncate(self):
        self.table.insert_pylist([[1, None]], ["string", "int"])
        self.assertEqual(self.table.count(), 1)

        self.table.truncate()
        self.assertEqual(self.table.count(), 0)
