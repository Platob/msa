__all__ = [
    "prepare_insert_statement",
    "prepare_insert_batch_statement"
]


def prepare_insert_statement(
    table_name: str,
    columns: list[str],
    tablock: bool = False
):
    return "INSERT INTO %s%s([%s]) VALUES (%s)" % (
        table_name,
        "WITH(TABLOCKX)" if tablock else "",
        "],[".join(columns),
        ",".join(("?" for _ in range(len(columns))))
    )


def prepare_insert_batch_statement(
    table_name: str,
    columns: list[str],
    tablock: bool = False,
    commit_size: int = 1
):
    values = "(%s)" % ",".join(("?" for _ in range(len(columns))))
    return "INSERT INTO %s%s([%s]) VALUES %s" % (
        table_name,
        "WITH(TABLOCKX)" if tablock else "",
        "],[".join(columns),
        ",".join((values for _ in range(commit_size)))
    )
