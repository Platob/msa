from pyarrow import RecordBatch

from tests import MSSQLTestCase


class ForeignTableTests(MSSQLTestCase):
    server = MSSQLTestCase.get_test_pyodbc_server()

    def setUp(self) -> None:
        MSSQLTestCase.create_test_table(self.server)
        MSSQLTestCase.create_foreign_test_table(self.server)
        self.table = self.server.connect().table(MSSQLTestCase.PYMSA_UNITTEST)
        self.foreign_table = self.server.connect().table(MSSQLTestCase.FOREIGN_PYMSA_UNITTEST)

    def tearDown(self) -> None:
        self.foreign_table.drop()
        self.table.drop()

    def test_insert_check_constraint(self):
        with self.server.cursor() as cursor:
            cursor.insert_pylist(
                self.table,
                [["str", 0]],
                ["string", "int"]
            )
            row = cursor.execute("select top 10 id, string, int from %s" % self.table).fetchall()[0]

        with self.server.cursor() as cursor:
            cursor.insert_pylist(
                self.foreign_table,
                [[row[0], "str"]],
                ["foreignid", "string"]
            )
            trusted = cursor.execute("select name, is_not_trusted from sys.foreign_keys").fetchall()

        self.assertEqual(
            [False for _ in trusted], [_[1] for _ in trusted]
        )

    def test_insert_nocheck_constraint(self):
        with self.server.cursor() as cursor:
            cursor.insert_arrow(
                self.table,
                RecordBatch.from_pydict({"string": ["str"]})
            )
            row = cursor.execute("select top 10 id, string, int from %s" % self.table).fetchall()[0]

        with self.server.connect() as connection:
            connection.timeout = 1
            with connection.cursor() as cursor:
                cursor.insert_arrow(
                    self.foreign_table,
                    RecordBatch.from_pydict({"string": ["str"], "foreignid": [0]}),
                    delayed_check_constraints=True,
                    check_constraints=False
                )
                trusted = cursor.execute("select name, is_not_trusted from sys.foreign_keys").fetchall()

            self.assertEqual(
                [True for _ in trusted], [_[1] for _ in trusted]
            )

    def test_insert_arrow_delayed_constraint(self):
        with self.server.cursor() as cursor:
            cursor.insert_arrow(
                self.table,
                RecordBatch.from_pydict({"string": ["str"]}),
                delayed_check_constraints=True
            )
            row = cursor.execute("select top 10 id, string, int from %s" % self.table).fetchall()[0]

        with self.server.connect() as connection:
            connection.timeout = 1
            with connection.cursor() as cursor:
                cursor.insert_arrow(
                    self.foreign_table,
                    RecordBatch.from_pydict(
                        {"string": ["str"], "foreignid": [row[0]]}
                    ),
                    delayed_check_constraints=True
                )
                trusted = cursor.execute("select name, is_not_trusted from sys.foreign_keys").fetchall()

            self.assertEqual(
                [False for _ in trusted], [_[1] for _ in trusted]
            )
