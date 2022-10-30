from tests import MSSQLTestCase


class PyMSSQLTableTests(MSSQLTestCase):
    PYMSA_UNITTEST = "PYMSA_UNITTEST"
    server = MSSQLTestCase.get_test_pymssql_server()

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
