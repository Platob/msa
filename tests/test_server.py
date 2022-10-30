from tests import MSSQLTestCase


class MSSQLServerTests(MSSQLTestCase):
    server = MSSQLTestCase.get_test_pyodbc_server()

    def test_connect(self):
        connection = self.server.connect()
        assert not connection.closed
        connection.close()
        assert connection.closed

    def test_with_connect(self):
        with self.server.connect() as connection:
            assert not connection.closed
        assert connection.closed

    def test_cursor(self):
        cursor = self.server.connect()
        assert not cursor.closed
        cursor.close()
        assert cursor.closed

    def test_with_cursor(self):
        with self.server.cursor() as cursor:
            assert not cursor.closed
        assert cursor.closed
