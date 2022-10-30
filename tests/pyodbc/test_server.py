from tests import MSSQLTestCase


class PyODBBCServerTests(MSSQLTestCase):
    server = MSSQLTestCase.get_test_pyodbc_server()

    def test_connect(self):
        connection = self.server.connect()
        assert not connection.closed
        connection.close()
        assert connection.closed
