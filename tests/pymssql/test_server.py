from tests import MSSQLTestCase


class PyMSSQLServerTests(MSSQLTestCase):
    server = MSSQLTestCase.get_test_pymssql_server()

    def test_connect(self):
        connection = self.server.connect()
        assert not connection.closed
        connection.close()
        assert connection.closed
