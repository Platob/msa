import unittest

from msa import MSSQL


class MSSQLTestCase(unittest.TestCase):
    PYMSA_UNITTEST = "PYMSA_UNITTEST"

    @staticmethod
    def get_test_pymssql_server():
        from msa.pymssql import PyMSSQL

        return PyMSSQL(
            host='localhost',
            database="master"
        )

    @staticmethod
    def get_test_pyodbc_server():
        from msa.odbc import PyODBC

        return PyODBC(
            uri="Server=localhost\SQLEXPRESS01;Database=master;DRIVER={ODBC Driver 18 for SQL Server};"
                "Trusted_Connection=yes;TrustServerCertificate=YES;"
        )

    @classmethod
    def create_test_table(cls, server: MSSQL):
        with server.cursor() as c:
            try:
                c.execute(f"DROP TABLE {cls.PYMSA_UNITTEST}")
            except Exception:
                pass
            c.execute(f"""CREATE TABLE {cls.PYMSA_UNITTEST} (
    int int,
    smallint smallint,
    tinyint tinyint,
    bigint bigint,
    bit bit,
    decimal decimal(15,5),
    float float,
    real real,
    money money,
    small_money smallmoney,
    date date,
    datetime datetime,
    datetime2 datetime2,
    smalldatetime smalldatetime,
    time time,
    string varchar(64) not null,
    binary varbinary(64),
    uniqueidentifier uniqueidentifier,
    datetime_offset DATETIMEOFFSET,
    ntext ntext,
    image image,
    char char,
    nchar nchar
)""")
            c.commit()
