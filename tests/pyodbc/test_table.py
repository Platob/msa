import datetime

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
            c.execute(f"""INSERT INTO {self.PYMSA_UNITTEST} (int, string, date)
            VALUES (1, 'test', '2022-10-20'), (null, 'test', null)""")
            c.commit()
            result = c.execute(f"SELECT string, int, date from {self.PYMSA_UNITTEST}").fetchall()
            result = [list(row) for row in result]

        expected = [
            ["test", 1, datetime.date(2022, 10, 20)],
            ["test", None, None]
        ]

        self.assertEqual(result, expected)

    def test_fetch_arrow_batches_empty(self):
        with self.server.cursor() as c:
            c.execute(f"TRUNCATE TABLE {self.PYMSA_UNITTEST}")
            c.commit()
            c.execute(f"SELECT int, string, date, float, real from {self.PYMSA_UNITTEST}")
            result = list(c.fetch_arrow_batches(10))[0]

        expected = pyarrow.RecordBatch.from_arrays([
            pyarrow.array([]),
            pyarrow.array([]),
            pyarrow.array([]),
            pyarrow.array([]),
            pyarrow.array([])
        ], schema=pyarrow.schema([
            pyarrow.field("int", pyarrow.int64(), nullable=True),
            pyarrow.field("string", pyarrow.string(), nullable=False),
            pyarrow.field("date", pyarrow.date32(), nullable=True),
            pyarrow.field("float", pyarrow.float64(), nullable=True),
            pyarrow.field("real", pyarrow.float32(), nullable=True)
        ]))

        self.assertEqual(expected, result)

    def test_fetch_arrow_batches(self):
        with self.server.cursor() as c:
            c.execute(f"TRUNCATE TABLE {self.PYMSA_UNITTEST}")
            c.execute(f"""INSERT INTO {self.PYMSA_UNITTEST} (int, string, date)
            VALUES (1, 'test', '2022-10-20'), (null, 'test', null)""")
            c.commit()
            c.execute(f"SELECT int, string, date, float, real from {self.PYMSA_UNITTEST}")
            result = list(c.fetch_arrow_batches())[0]

        expected = pyarrow.RecordBatch.from_arrays([
            pyarrow.array([1, None]),
            pyarrow.array(['test', 'test']),
            pyarrow.array([datetime.date(2022, 10, 20), None]),
            pyarrow.array([None, None]),
            pyarrow.array([None, None])
        ], schema=pyarrow.schema([
            pyarrow.field("int", pyarrow.int64(), nullable=True),
            pyarrow.field("string", pyarrow.string(), nullable=False),
            pyarrow.field("date", pyarrow.date32(), nullable=True),
            pyarrow.field("float", pyarrow.float64(), nullable=True),
            pyarrow.field("real", pyarrow.float32(), nullable=True)
        ]))

        self.assertEqual(expected, result)

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
            pyarrow.field("int", pyarrow.int64(), nullable=True),
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
            pyarrow.field("int", pyarrow.int64(), nullable=True),
            pyarrow.field("string", pyarrow.string(), nullable=False),
            pyarrow.field("date", pyarrow.date32(), nullable=True),
            pyarrow.field("float", pyarrow.float64(), nullable=True),
            pyarrow.field("real", pyarrow.float32(), nullable=True)
        ]))

        self.assertEqual(expected, result)
