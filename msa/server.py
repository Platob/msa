__all__ = ["MSSQL"]

import os
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Any, Iterable, Union

from pyarrow.fs import LocalFileSystem, FileSystem, FileInfo

from msa.connection import Connection
from msa.cursor import Cursor
from .table import SQLTable
from msa.utils.arrow import iter_dir_files


def return_iso(o):
    return o


class MSSQL:

    def __init__(
        self,
        package: str
    ):
        self.package = package

    @abstractmethod
    def connect(self, **kwargs) -> Connection:
        raise NotImplemented

    def cursor(self, connect: dict = {}, **kwargs) -> Cursor:
        return self.connect(**connect).cursor(**kwargs)

    def cursor_execute(
        self,
        method: str,
        cursor_wrapper: Callable[[Cursor], Any] = return_iso,
        result_wrapper: Callable[[Any], Any] = return_iso,
        arguments: tuple[list, dict] = ()
    ):
        if arguments:
            return result_wrapper(getattr(cursor_wrapper(self.cursor()), method)(*arguments[0], **arguments[1]))
        else:
            return result_wrapper(getattr(cursor_wrapper(self.cursor()), method)())

    def execute(
        self,
        method: str,
        cursor_wrapper: Callable = return_iso,
        result_wrapper: Callable = return_iso,
        concurrency: ThreadPoolExecutor = ThreadPoolExecutor(os.cpu_count()),
        arguments: Iterable[tuple[list, dict]] = (),
        timeout: int = None
    ):
        return (
            task.result()
            for task in as_completed([
                concurrency.submit(
                    self.cursor_execute,
                    method=method,
                    cursor_wrapper=cursor_wrapper,
                    result_wrapper=result_wrapper,
                    arguments=argument
                ) for argument in arguments
            ], timeout=timeout)
        )

    def insert_parquet_dir(
        self,
        table: Union[str, tuple[str, str, str], SQLTable],
        base_dir: str,
        filesystem: FileSystem = LocalFileSystem(),
        filter_file: Callable[[FileInfo], bool] = lambda x: True,
        cursor_wrapper: Callable = return_iso,
        result_wrapper: Callable = return_iso,
        concurrency: ThreadPoolExecutor = ThreadPoolExecutor(os.cpu_count()),
        timeout: int = None,
        **insert_parquet_file
    ):
        # persist table data
        if not isinstance(table, SQLTable):
            with self.cursor() as c:
                table = c.safe_table_or_view(table)
        table = (table.catalog, table.schema, table.name)

        insert_parquet_file["filesystem"] = filesystem

        return self.execute(
            "insert_parquet_file",
            cursor_wrapper=cursor_wrapper,
            result_wrapper=result_wrapper,
            concurrency=concurrency,
            timeout=timeout,
            arguments=[
                ([table, ofs.path], insert_parquet_file)
                for ofs in iter_dir_files(filesystem, base_dir)
                if filter_file(ofs) and ofs.size > 0
            ]
        )
