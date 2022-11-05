__all__ = [
    "DEFAULT_BATCH_ROW_SIZE",
    "DEFAULT_SAFE_MODE",
    "WITH_PANDAS"
]

DEFAULT_BATCH_ROW_SIZE = 10
DEFAULT_SAFE_MODE = True

try:
    import pandas
    WITH_PANDAS = True
except ImportError:
    WITH_PANDAS = False
