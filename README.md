# msa
MSSQL with PyArrow

Clients
- pyodbc
- pymssql

## Begin

```python
from msa.pymssql import PyMSSQL
from msa.odbc import PyODBC

server = PyMSSQL(
    server="localhost", database="master"
)

server = PyODBC(
    uri="Server=localhost\SQLEXPRESS01;Database=master;DRIVER={ODBC Driver 18 for SQL Server};"
        "Trusted_Connection=yes;TrustServerCertificate=YES;"
)

with server.connect() as connection:
    with connection.cursor() as cursor:        
        cursor.execute("select * from table")
        for row in cursor.rows(10): # Generator[tuple]
            print(row)
        
    with connection.cursor() as cursor:        
        cursor.execute("select * from table")
        print(cursor.schema_arrow)
        data = cursor.fetch_arrow(10) # pyarrow.Table
        
    with connection.cursor() as cursor:
        cursor.execute("select * from table")
        data = cursor.fetch_arrow_batches(10) # Generator[pyarrow.RecordBatch]
    
    table = connection.table(name="table", catalog="master", schema="dbo")
    
    table.insert_pylist(
        rows=[["string", 1]],
        columns=["string", "int"],
        commit=True
    )
    table.insert_arrow(
        data=[], # RecordBatch, RecordBatchReader or Iterable[RecordBatch]
        cast=True, # cast to table.schema_arrow
        safe=True, # safe cast
        commit=True
    )
```


## PyODBC

#### Python dependency
 In command shell within your python environment
```shell
pip install pyodbc>=4
```

#### ODBC Driver

ODBC Driver from [Microsoft ODBC](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16)

Update the `DRIVER={installed ODBC driver name}'` in the uri

## PyMSSQL

#### Python dependency
 In command shell within your python environment
```shell
pip install pymssql>=2.2
```