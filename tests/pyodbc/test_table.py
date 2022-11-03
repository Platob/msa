import datetime

import numpy
import pyarrow

from tests import MSSQLTestCase


class PyODBCTableTests(MSSQLTestCase):
    PYMSA_UNITTEST = "PYMSA_UNITTEST"
    server = MSSQLTestCase.get_test_pyodbc_server()

    def setUp(self):
        self.create_test_table(self.server)

    def tearDown(self) -> None:
        with self.server.cursor() as c:
            c.execute(f"DROP TABLE {self.PYMSA_UNITTEST}")
            c.commit()

    def test_fetchall_empty(self):
        with self.server.cursor() as c:
            result = c.execute(f"SELECT * from {self.PYMSA_UNITTEST}").fetchall()
        self.assertEqual(result, [])

    def test_fetchall_with_values(self):
        with self.server.cursor() as c:
            c.execute(f"TRUNCATE TABLE {self.PYMSA_UNITTEST}")
            c.execute(f"""INSERT INTO {self.PYMSA_UNITTEST} (int, string, date, uniqueidentifier)
            VALUES (1, 'test', '2022-10-20', 'cac13e7f-bcf2-4830-9e9d-9cc341d25e3e'),
                (null, 'test', null, null)""")
            c.commit()
            result = c.execute(f"SELECT string, int, date, uniqueidentifier from {self.PYMSA_UNITTEST}").fetchall()
            result = [list(row) for row in result]

        expected = [
            ["test", 1, datetime.date(2022, 10, 20), 'CAC13E7F-BCF2-4830-9E9D-9CC341D25E3E'],
            ["test", None, None, None]
        ]

        self.assertEqual(result, expected)

    def test_fetchall_with_datetime_offset(self):
        with self.server.cursor() as c:
            c.execute(f"TRUNCATE TABLE {self.PYMSA_UNITTEST}")
            c.execute(f"""INSERT INTO {self.PYMSA_UNITTEST} (string, datetime_offset)
            VALUES ('test', '2017-03-16 10:35:18.5 -06:00')""")
            c.commit()

            result = c.execute(f"SELECT string, datetime_offset from {self.PYMSA_UNITTEST}").fetchall()
            result = [list(row) for row in result]

        expected = [
            ["test", numpy.datetime64('2017-03-16T16:35:18.500000000')]
        ]

        self.assertEqual(result, expected)

    def test_fetchall_with_datetime2(self):
        with self.server.cursor() as c:
            c.execute(f"TRUNCATE TABLE {self.PYMSA_UNITTEST}")
            c.execute(f"""INSERT INTO {self.PYMSA_UNITTEST} (string, datetime2)
            VALUES ('test', '2017-03-16 10:35:18.12345678')""")
            c.commit()

            result = c.execute(f"SELECT string, datetime2 from {self.PYMSA_UNITTEST}").fetchall()
            result = [list(row) for row in result]

        expected = [
            ["test", numpy.datetime64('2017-03-16T10:35:18.123456800')]
        ]

        self.assertEqual(result, expected)

    def test_fetch_arrow_batches_empty(self):
        with self.server.cursor() as c:
            c.execute(f"TRUNCATE TABLE {self.PYMSA_UNITTEST}")
            c.commit()
            c.execute(f"SELECT int, string, date, float, real, uniqueidentifier from {self.PYMSA_UNITTEST}")
            result = list(c.fetch_arrow_batches(10))[0]

        expected = pyarrow.RecordBatch.from_arrays([
            pyarrow.array([]),
            pyarrow.array([]),
            pyarrow.array([]),
            pyarrow.array([]),
            pyarrow.array([]),
            pyarrow.array([])
        ], schema=pyarrow.schema([
            pyarrow.field("int", pyarrow.int32(), nullable=True),
            pyarrow.field("string", pyarrow.string(), nullable=False),
            pyarrow.field("date", pyarrow.date32(), nullable=True),
            pyarrow.field("float", pyarrow.float64(), nullable=True),
            pyarrow.field("real", pyarrow.float32(), nullable=True),
            pyarrow.field("uniqueidentifier", pyarrow.string(), nullable=True)
        ]))

        self.assertEqual(expected, result)

    def test_fetch_arrow_batches(self):
        with self.server.cursor() as c:
            c.execute(f"TRUNCATE TABLE {self.PYMSA_UNITTEST}")
            c.execute(f"""INSERT INTO {self.PYMSA_UNITTEST} (int, string, date, datetime, datetime2)
            VALUES (1, 'test', '2022-10-20', '2017-03-16 10:35:18.123', '2017-03-16 10:35:18.12345678'),
            (null, 'test', null, null, null)""")
            c.commit()
            c.execute(f"SELECT int, string, date, float, real, datetime, datetime2 from {self.PYMSA_UNITTEST}")
            result = list(c.fetch_arrow_batches())[0]

        expected = pyarrow.RecordBatch.from_arrays([
            pyarrow.array([1, None]),
            pyarrow.array(['test', 'test']),
            pyarrow.array([datetime.date(2022, 10, 20), None]),
            pyarrow.array([None, None]),
            pyarrow.array([None, None]),
            pyarrow.array([datetime.datetime(2017, 3, 16, 10, 35, 18, 123000), None]).cast(pyarrow.timestamp("ms"), False),
            pyarrow.array([numpy.datetime64('2017-03-16T10:35:18.123456800'), None])
        ], schema=pyarrow.schema([
            pyarrow.field("int", pyarrow.int32(), nullable=True),
            pyarrow.field("string", pyarrow.string(), nullable=False),
            pyarrow.field("date", pyarrow.date32(), nullable=True),
            pyarrow.field("float", pyarrow.float64(), nullable=True),
            pyarrow.field("real", pyarrow.float32(), nullable=True),
            pyarrow.field("datetime", pyarrow.timestamp("ms"), nullable=True),
            pyarrow.field("datetime2", pyarrow.timestamp("ns"), nullable=True)
        ]))

        self.assertEqual(
            pyarrow.Table.from_batches([expected]),
            pyarrow.Table.from_batches([result])
        )

    def test_fetch_arrow_empty(self):
        with self.server.cursor() as c:
            c.execute(f"TRUNCATE TABLE {self.PYMSA_UNITTEST}")
            c.commit()
            c.execute(f"SELECT int, string, date, float, real from {self.PYMSA_UNITTEST}")
            result = c.fetch_arrow()

        expected = pyarrow.Table.from_arrays([
            pyarrow.array([]),
            pyarrow.array([]),
            pyarrow.array([]),
            pyarrow.array([]),
            pyarrow.array([])
        ], schema=pyarrow.schema([
            pyarrow.field("int", pyarrow.int32(), nullable=True),
            pyarrow.field("string", pyarrow.string(), nullable=False),
            pyarrow.field("date", pyarrow.date32(), nullable=True),
            pyarrow.field("float", pyarrow.float64(), nullable=True),
            pyarrow.field("real", pyarrow.float32(), nullable=True)
        ]))

        self.assertEqual(expected, result)

    def test_fetch_arrow(self):
        with self.server.cursor() as c:
            c.execute(f"TRUNCATE TABLE {self.PYMSA_UNITTEST}")
            c.execute(f"""INSERT INTO {self.PYMSA_UNITTEST} (int, string, date)
            VALUES (1, 'test', '2022-10-20'), (null, 'test', null)""")
            c.commit()
            c.execute(f"SELECT int, string, date, float, real from {self.PYMSA_UNITTEST}")
            result = c.fetch_arrow()

        expected = pyarrow.Table.from_arrays([
            pyarrow.array([1, None]),
            pyarrow.array(['test', 'test']),
            pyarrow.array([datetime.date(2022, 10, 20), None]),
            pyarrow.array([None, None]),
            pyarrow.array([None, None])
        ], schema=pyarrow.schema([
            pyarrow.field("int", pyarrow.int32(), nullable=True),
            pyarrow.field("string", pyarrow.string(), nullable=False),
            pyarrow.field("date", pyarrow.date32(), nullable=True),
            pyarrow.field("float", pyarrow.float64(), nullable=True),
            pyarrow.field("real", pyarrow.float32(), nullable=True)
        ]))

        self.assertEqual(expected, result)
