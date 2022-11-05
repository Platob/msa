from tests import MSSQLTestCase


class PyODBBCServerTests(MSSQLTestCase):
    server = MSSQLTestCase.get_test_pyodbc_server()

    def test_connect(self):
        connection = self.server.connect()
        assert not connection.closed
        connection.close()
        assert connection.closed

    def test_uri_map(self):
        self.assertEqual(
            {
                'DRIVER': '{ODBC Driver 18 for SQL Server}',
                'Database': 'master',
                'Server': 'localhost\\SQLEXPRESS01',
                'TrustServerCertificate': 'YES',
                'Trusted_Connection': 'yes'
            },
            self.server.uri_map
        )