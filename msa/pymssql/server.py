__all__ = ["PyMSSQL"]

import pymssql

from msa.pymssql.connection import PyMSSQLConnection
from msa.server import MSSQL


class PyMSSQL(MSSQL):

    def __init__(
        self,
        server: str = '.',
        user: str = None,
        password: str = None,
        database: str = '',
        timeout: int = 0,
        login_timeout: int = 60,
        charset: str = 'UTF-8',
        as_dict: bool = False,
        host: str = '',
        appname=None,
        port: str = '1433',
        conn_properties=None,
        autocommit=False,
        tds_version=None
    ):
        self.server = server
        self.user = user
        self.password = password
        self.database = database
        self.timeout = timeout
        self.login_timeout = login_timeout
        self.charset = charset
        self.as_dict = as_dict
        self.host = host
        self.appname = appname
        self.port = port
        self.conn_properties = conn_properties
        self.autocommit = autocommit
        self.tds_version = tds_version
        super(PyMSSQL, self).__init__(package="pymssql")

    def connect(self, **kwargs) -> PyMSSQLConnection:
        return PyMSSQLConnection(
            server=self,
            raw=pymssql.connect(
                server=self.server,
                user=self.user,
                password=self.password,
                database=self.database,
                timeout=self.timeout,
                login_timeout=self.login_timeout,
                charset=self.charset,
                as_dict=self.as_dict,
                host=self.host,
                appname=self.appname,
                port=self.port,
                conn_properties=self.conn_properties,
                autocommit=self.autocommit,
                tds_version=self.tds_version
            )
        )
