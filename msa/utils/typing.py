__all__ = [
    "ArrowData"
]

from typing import Iterable, Union

from pyarrow import RecordBatch, Table, RecordBatchReader

ArrowData = Union[
    RecordBatch, Table,
    Iterable[RecordBatch], RecordBatchReader
]
