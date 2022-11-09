import pyarrow as pa
from pyarrow import schema

from tests import MSSQLTestCase


class ViewTests(MSSQLTestCase):
    server = MSSQLTestCase.get_test_pyodbc_server()

    def setUp(self) -> None:
        MSSQLTestCase.create_test_view(self.server)
        self.table = self.server.connect().view(MSSQLTestCase.vPYMSA_UNITTEST)

    def tearDown(self) -> None:
        self.table.drop()

    def test_view_schema_arrow(self):
        expected = schema([
            pa.field("int", pa.int32(), True),
            pa.field("decimal", pa.decimal128(15, 5), True),
            pa.field("datetime2", pa.timestamp("ns"), True),
            pa.field("string", pa.large_string(), False)
        ])

        self.assertEqual(self.table.schema_arrow, expected)
