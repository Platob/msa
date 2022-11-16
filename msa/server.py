__all__ = ["MSSQL"]

import os
import tqdm

from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Any, Iterable, Union, Sized, Generator

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

    def cursor_apply(
        self,
        method: Union[str, Callable],
        cursor_wrapper: Callable[[Cursor], Cursor] = return_iso,
        result_wrapper: Callable[[Any], Any] = return_iso,
        arguments: tuple[list, dict] = ()
    ):
        if not isinstance(method, str) or not callable(method):
            # all in first arg
            method, cursor_wrapper, result_wrapper, arguments = method

        with cursor_wrapper(self.cursor()) as cursor:
            if isinstance(method, str):
                method = getattr(cursor.__class__, method)

            if arguments:
                return result_wrapper(method(cursor, *arguments[0], **arguments[1]))
            else:
                return result_wrapper(method(cursor))

    def map(
        self,
        method: Union[str, Callable],
        cursor_wrapper: Callable = return_iso,
        result_wrapper: Callable = return_iso,
        concurrency: int = os.cpu_count(),
        arguments: Iterable[tuple[list, dict]] = (),
        arguments_fetch_size: int = os.cpu_count(),
        timeout: int = None
    ) -> Generator[Any, None, None]:
        with ThreadPoolExecutor(concurrency) as pool:
            for _ in tqdm.tqdm(
                pool.map(
                    self.cursor_apply,
                    (
                        (method, cursor_wrapper, result_wrapper, arg) for arg in arguments
                    ),
                    timeout=timeout,
                    chunksize=arguments_fetch_size
                ),
                total=len(arguments) if isinstance(arguments, Sized) else None
            ):
                yield _

    def insert_parquet_dir(
        self,
        table: Union[str, tuple[str, str, str], SQLTable],
        base_dir: str,
        filesystem: FileSystem = LocalFileSystem(),
        filter_file: Callable[[FileInfo], bool] = lambda x: True,
        cursor_wrapper: Callable = return_iso,
        result_wrapper: Callable = return_iso,
        concurrency: int = os.cpu_count(),
        timeout: int = None,
        arguments_fetch_size: int = os.cpu_count(),
        **insert_parquet_file
    ):
        # persist table data
        if not isinstance(table, SQLTable):
            with self.cursor() as c:
                table = c.safe_table_or_view(table)
        table = (table.catalog, table.schema, table.name)

        insert_parquet_file["filesystem"] = filesystem

        return self.map(
            "insert_parquet_file",
            cursor_wrapper=cursor_wrapper,
            result_wrapper=result_wrapper,
            concurrency=concurrency,
            arguments_fetch_size=arguments_fetch_size,
            timeout=timeout,
            arguments=[
                ([table, ofs.path], insert_parquet_file)
                for ofs in iter_dir_files(filesystem, base_dir)
                if filter_file(ofs) and ofs.size > 0
            ]
        )
