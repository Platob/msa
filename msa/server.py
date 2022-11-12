__all__ = ["MSSQL"]

import os
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Any

from msa.connection import Connection
from msa.cursor import Cursor


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
        arguments: list[tuple[list, dict]] = (),
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
