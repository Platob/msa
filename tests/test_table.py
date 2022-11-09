import datetime
import decimal
import io
import os
import tempfile

import numpy
import pyarrow
import pyarrow as pa
from pyarrow import schema, RecordBatchReader

from msa.table import SQLIndex
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
            pa.field("smallint", pa.int16(), True),
            pa.field("tinyint", pa.int8(), True),
            pa.field("bigint", pa.int64(), True),
            pa.field("bit", pa.bool_(), True),
            pa.field("decimal", pa.decimal128(15, 5), True),
            pa.field("float", pa.float64(), True),
            pa.field("real", pa.float32(), True),
            pa.field("money", pa.float64(), True),
            pa.field("small_money", pa.float32(), True),
            pa.field("date", pa.date32(), True),
            pa.field("datetime", pa.timestamp("ms"), True),
            pa.field("datetime2", pa.timestamp("ns"), True),
            pa.field("smalldatetime", pa.timestamp("s"), True),
            pa.field("time", pa.time64("ns"), True),
            pa.field("string", pa.large_string(), False),
            pa.field("binary", pa.large_binary(), True),
            pa.field("uniqueidentifier", pa.string(), True),
            pa.field("datetime_offset", pa.timestamp("ns", "UTC"), True),
            pa.field("ntext", pa.large_string(), True),
            pa.field("image", pa.large_binary(), True),
            pa.field("char", pa.large_string(), True),
            pa.field("nchar", pa.large_string(), True)
        ])

        self.assertEqual(self.table.schema_arrow, expected)

    def test_prepare_insert_statement(self):
        self.assertEqual(
            "INSERT INTO [master].[dbo].[PYMSA_UNITTEST]([a],[AbC Column]) VALUES (?,?)",
            self.table.prepare_insert_statement(["a", "AbC Column"])
        )

    def test_prepare_insert_batch_statement(self):
        self.assertEqual(
            "INSERT INTO [master].[dbo].[PYMSA_UNITTEST]([a],[AbC Column]) VALUES (?,?)",
            self.table.prepare_insert_batch_statement(["a", "AbC Column"], commit_size=1)
        )
        self.assertEqual(
            "INSERT INTO [master].[dbo].[PYMSA_UNITTEST]([a],[AbC Column]) VALUES (?,?),(?,?),(?,?)",
            self.table.prepare_insert_batch_statement(["a", "AbC Column"], commit_size=3)
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
        self.assertEqual(
            self.table.full_name,
            str(self.table)
        )

    def test_get_field(self):
        self.assertEqual(
            self.table.field("string"),
            pa.field("string", pa.large_string(), False)
        )

    def test_get_field_case_insensitive(self):
        self.assertEqual(
            self.table.field("String"),
            pa.field("string", pa.large_string(), False)
        )
        self.assertEqual(
            self.table["String"],
            pa.field("string", pa.large_string(), False)
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

    def test_insert_pylist_large_values_batch(self):
        data = [
            ["test", i, b"bin", datetime.datetime.now()] for i in range(10001)
        ]
        self.table.truncate()
        self.table.insert_pylist(
            data,
            ["string", "int", "binary", "datetime2"],
            tablock=False,
            fast_executemany=False,
            commit_size=1000  # max 1000
        )

        with self.server.cursor() as c:
            c.execute(f"select string, int, binary, datetime2 from {self.PYMSA_UNITTEST}")
            result = c.fetchall(100)

        self.assertEqual(len(data), len(result))

    def test_insert_pylist_values_batch(self):
        data = [
            ["test", i, b"bin"] for i in range(101)
        ]
        self.table.truncate()
        self.table.insert_pylist(
            data,
            ["string", "int", "binary"],
            tablock=False,
            fast_executemany=False,
            commit_size=10  # max 1000
        )

        with self.server.cursor() as c:
            c.execute(f"select string, int, binary from {self.PYMSA_UNITTEST}")
            result = c.fetchall(100)

        self.assertEqual(data, [list(_) for _ in result])

    def test_insert_pylist_values_batch_tablock(self):
        data = [
            ["test", i, b"bin"] for i in range(10001)
        ]
        self.table.truncate()
        self.table.insert_pylist(
            data,
            ["string", "int", "binary"],
            tablock=True,
            commit_size=1000  # max 1000
        )

        self.assertEqual(
            data,
            sorted([
                list(_)
                for _ in self.server.cursor().execute(f"select string, int, binary from {self.PYMSA_UNITTEST}").fetchall()
            ])
        )

    def test_insert_pylist_decimal(self):
        self.table.truncate()
        self.table.insert_pylist([[1, decimal.Decimal("10.30000")]], ["string", "decimal"])
        self.assertEqual(
            [["1", decimal.Decimal("10.30000")]],
            [
                list(_)
                for _ in self.server.cursor().execute(f"select string, decimal from {self.PYMSA_UNITTEST}").fetchall()
            ]
        )

    def test_insert_large_pylist(self):
        data = [
            ["1", None] for _ in range(10000)
        ]
        self.table.truncate()
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
            pyarrow.array([b"test", None]),
            pyarrow.array([10.3, None])
        ], schema=pyarrow.schema([
            pyarrow.field("int", pyarrow.int32(), nullable=True),
            pyarrow.field("string", pyarrow.string(), nullable=False),
            pyarrow.field("date", pyarrow.date32(), nullable=True),
            pyarrow.field("float", pyarrow.float64(), nullable=True),
            pyarrow.field("real", pyarrow.float32(), nullable=True),
            pyarrow.field("datetime", pyarrow.timestamp("ms"), nullable=True),
            pyarrow.field("datetime2", pyarrow.timestamp("ns"), nullable=True),
            pyarrow.field("binary", pyarrow.binary(), nullable=True),
            pyarrow.field("decimal", pyarrow.decimal128(15, 5), nullable=True)
        ]))

        self.table.truncate()
        self.table.insert_arrow(data, cast=False, safe=True, commit=True)

        result = [
            list(_)
            for _ in self.server.cursor().execute(
                f"select int, string, date, float, real, datetime, datetime2, binary, decimal from {self.PYMSA_UNITTEST}"
            ).fetchall()
        ]

        self.assertEqual(
            [
                [
                    1, 'test', datetime.date(2022, 10, 20), None, None,
                    datetime.datetime(2017, 3, 16, 10, 35, 18, 123000),
                    numpy.datetime64('2017-03-16T10:35:18.123456800'), b'test',
                    decimal.Decimal('10.30000')
                ],
                [
                    None, 'test', None, None, None, None, None, None, None
                ]
            ],
            result
        )

    def test_insert_pyarrow_batches(self):
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
        self.table.insert_arrow([data, data], cast=True, safe=True, commit=True, commit_size=10)

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
                    numpy.datetime64('2017-03-16T10:35:18.123456800'), b'test'
                ],
                [
                    None, 'test', None, None, None, None, None, None
                ],
                [
                    1, 'test', datetime.date(2022, 10, 20), None, None,
                    datetime.datetime(2017, 3, 16, 10, 35, 18, 123000),
                    numpy.datetime64('2017-03-16T10:35:18.123456800'), b'test'
                ],
                [
                    None, 'test', None, None, None, None, None, None
                ]
            ],
            result
        )

    def test_insert_pyarrow_record_batches(self):
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
        self.table.insert_arrow(
            RecordBatchReader.from_batches(data.schema, [data, data]), cast=False, safe=True, commit=True,
            commit_size=10
        )

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
                    numpy.datetime64('2017-03-16T10:35:18.123456800'), b'test'
                ],
                [
                    None, 'test', None, None, None, None, None, None
                ],
                [
                    1, 'test', datetime.date(2022, 10, 20), None, None,
                    datetime.datetime(2017, 3, 16, 10, 35, 18, 123000),
                    numpy.datetime64('2017-03-16T10:35:18.123456800'), b'test'
                ],
                [
                    None, 'test', None, None, None, None, None, None
                ]
            ],
            result
        )

    def test_bulk_insert_arrow(self):
        data = pyarrow.RecordBatch.from_arrays([
            pyarrow.array([datetime.date(2022, 10, 20), None]),
            pyarrow.array(['test', 'test']),
            pyarrow.array([None, None]),
            pyarrow.array([None, None]),
            pyarrow.array([datetime.datetime(2017, 3, 16, 10, 35, 18, 123000), None])
            .cast(pyarrow.timestamp("ms"), False),
            pyarrow.array([numpy.datetime64('2017-03-16T10:35:18.123456800'), None]),
            pyarrow.array([1, None]),
        ], schema=pyarrow.schema([
            pyarrow.field("date", pyarrow.date32(), nullable=True),
            pyarrow.field("string", pyarrow.string(), nullable=False),
            pyarrow.field("float", pyarrow.float64(), nullable=True),
            pyarrow.field("real", pyarrow.float32(), nullable=True),
            pyarrow.field("datetime", pyarrow.timestamp("ms"), nullable=True),
            pyarrow.field("datetime2", pyarrow.timestamp("ns"), nullable=True),
            pyarrow.field("int", pyarrow.int32(), nullable=True)
        ]))

        self.table.truncate()
        self.table.bulk_insert_arrow(
            data,
            safe=True, commit=True,
            delimiter=";",
            tablock=True
        )

        result = [
            list(_)
            for _ in self.server.cursor().execute(
                f"select int, string, date, float, real, datetime, datetime2 from {self.PYMSA_UNITTEST}"
            ).fetchall()
        ]

        self.assertEqual(
            [
                [
                    1, 'test', datetime.date(2022, 10, 20), None, None,
                    datetime.datetime(2017, 3, 16, 10, 35, 18, 123000),
                    numpy.datetime64('2017-03-16T10:35:18.123456800')
                ],
                [
                    None, 'test', None, None, None, None, None
                ]
            ],
            result
        )

    def test_bulk_insert_arrow_binary(self):
        data = pyarrow.RecordBatch.from_arrays([
            pyarrow.array(['test', 'test']),
            pyarrow.array([b"test", None]),
            pyarrow.array([b"image", None])
        ], schema=pyarrow.schema([
            pyarrow.field("string", pyarrow.string(), nullable=False),
            pyarrow.field("binary", pyarrow.binary(), nullable=True),
            pyarrow.field("image", pyarrow.binary(), nullable=True)
        ]))

        self.table.truncate()
        self.table.bulk_insert_arrow(
            data,
            cast=True, safe=True, commit=True
        )

        result = [
            list(_)
            for _ in self.server.cursor().execute(
                f"select string, binary, image from {self.PYMSA_UNITTEST}"
            ).fetchall()
        ]

        self.assertEqual(
            [
                ['test', b"test", b"image"],
                ['test', None, None]
            ],
            result
        )

    def test_truncate(self):
        self.table.insert_pylist([[1, None]], ["string", "int"])
        self.assertEqual(self.table.count(), 1)

        self.table.truncate()
        self.assertEqual(self.table.count(), 0)

    def test_insert_parquet_file_buffer(self):
        import pyarrow.parquet as p
        data = pyarrow.Table.from_arrays([
            pyarrow.array(['test', 'test']),
            pyarrow.array([b"test", None]),
            pyarrow.array([b"image", None]),
            pyarrow.array(['dropped', 'dropped'])
        ], schema=pyarrow.schema([
            pyarrow.field("string", pyarrow.string(), nullable=False),
            pyarrow.field("binary", pyarrow.binary(), nullable=True),
            pyarrow.field("image", pyarrow.binary(), nullable=True),
            pyarrow.field("dropped", pyarrow.string(), nullable=False)
        ]))

        buf = io.BytesIO()
        p.write_table(data, buf)
        buf.seek(0)

        self.table.truncate()
        self.table.insert_parquet_file(buf)

        result = [
            list(_)
            for _ in self.server.cursor().execute(
                f"select string, binary, image from {self.PYMSA_UNITTEST}"
            ).fetchall()
        ]

        self.assertEqual(
            [
                ['test', b"test", b"image"],
                ['test', None, None]
            ],
            result
        )

    def test_insert_parquet_dir(self):
        import pyarrow.parquet as p
        data0 = pyarrow.Table.from_arrays([
            pyarrow.array(['data0', 'data0']),
            pyarrow.array([b"data0", None])
        ], schema=pyarrow.schema([
            pyarrow.field("string", pyarrow.string(), nullable=False),
            pyarrow.field("binary", pyarrow.binary(), nullable=True)
        ]))

        data1 = pyarrow.Table.from_arrays([
            pyarrow.array(['data1', 'data1']),
            pyarrow.array([b"data1", None])
        ], schema=pyarrow.schema([
            pyarrow.field("string", pyarrow.string(), nullable=False),
            pyarrow.field("binary", pyarrow.binary(), nullable=True)
        ]))

        with tempfile.TemporaryDirectory() as base_dir:
            f0, f1 = os.path.join(base_dir, "part=0"), os.path.join(base_dir, "part=1")
            p.write_table(data0, f0)
            p.write_table(data1, f1)

            self.table.truncate()
            files = [
                _ for _ in self.table.insert_parquet_dir(base_dir)
            ]

        self.assertEqual(
            [f0.replace("\\", "/"), f1.replace("\\", "/")],  # windows path like C:\\
            [_.path for _ in files]
        )

        result = [
            list(_)
            for _ in self.server.cursor().execute(
                f"select string, binary, image from {self.PYMSA_UNITTEST}"
            ).fetchall()
        ]

        self.assertEqual(
            [['data0', b'data0', None],
             ['data0', None, None],
             ['data1', b'data1', None],
             ['data1', None, None]],
            result
        )

    def test_create_drop_index(self):
        with self.server.cursor() as c:
            c.create_table_index(
                table=self.table,
                columns=["string", "int"]
            )
            c.commit()

        self.assertEqual(
            {
                'IDX:string_int': SQLIndex(
                    index_id=0,
                    table=self.table, name='IDX:string_int', columns=['string', 'int'],
                    type='NONCLUSTERED', unique=False
                )
            },
            self.table.indexes
        )

        with self.server.cursor() as c:
            c.drop_table_index(
                table=self.table,
                name="IDX:string_int"
            )
            c.commit()

        self.assertEqual(
            {},
            self.table.indexes
        )

    def test_disable_rebuild_index(self):
        with self.server.connect() as connection:
            with connection.cursor() as c:
                c.create_table_index(
                    table=self.table,
                    name="test_index",
                    columns=["string", "int"]
                )

                try:
                    # disable
                    self.assertEqual(
                        False,
                        self.table.indexes["test_index"].disabled
                    )
                    c.disable_table_index(self.table, "test_index")
                    self.assertEqual(
                        True,
                        self.table.indexes["test_index"].disabled
                    )

                    # rebuild
                    c.rebuild_table_index(self.table, "test_index")
                    self.assertEqual(
                        False,
                        self.table.indexes["test_index"].disabled
                    )
                finally:
                    c.drop_table_index(
                        table=self.table,
                        name="test_index"
                    )

    def test_disable_rebuild_all_indexes(self):
        with self.server.connect() as connection:
            with connection.cursor() as c:
                c.create_table_index(
                    table=self.table,
                    name="test_index",
                    columns=["string", "int"]
                )

                try:
                    # disable
                    self.assertEqual(
                        False,
                        self.table.indexes["test_index"].disabled
                    )
                    c.disable_table_all_indexes(self.table)
                    self.assertEqual(
                        True,
                        self.table.indexes["test_index"].disabled
                    )

                    # rebuild
                    c.rebuild_table_all_indexes(self.table)
                    self.assertEqual(
                        False,
                        self.table.indexes["test_index"].disabled
                    )
                finally:
                    c.drop_table_index(
                        table=self.table,
                        name="test_index"
                    )
