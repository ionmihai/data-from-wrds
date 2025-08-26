import os
from typing import Sequence, Optional, Union, List, Dict
from contextlib import contextmanager

import pandas as pd
import wrds


def load_wrds_username() -> str:
    """Load WRDS username from environment variables."""
    v = os.getenv("WRDS_USERNAME")
    if not v: raise RuntimeError("WRDS_USERNAME not found")
    return v


def load_wrds_password() -> str:
    """Load WRDS password from environment variables."""
    v = os.getenv("WRDS_PASSWORD")
    if not v: raise RuntimeError("WRDS_PASSWORD not found")
    return v


@contextmanager
def wrds_connection():
    """Create and yield a WRDS database connection, closing it afterwards."""
    db = wrds.Connection(
        wrds_username=load_wrds_username(),
        wrds_password=load_wrds_password()
    )
    try:
        yield db
    finally:
        db.close()


def run_wrds_query(
    sql_string: str,
    params: Optional[Sequence] = None,
    coerce_float: bool = True,
    date_cols: Optional[Union[List[str], Dict[str, str]]] = None,
    chunksize: Optional[int] = 500_000,
    dtype_backend: str = "pyarrow",  # default in wrds is "numpy_nullable"
) -> pd.DataFrame:
    """Execute a raw SQL query on WRDS and return results as a DataFrame."""
    with wrds_connection() as db:
        return db.raw_sql(
            sql=sql_string,
            params=params,
            coerce_float=coerce_float,
            date_cols=date_cols,
            chunksize=chunksize,
            dtype_backend=dtype_backend,
        )


def get_wrds_table(
    library: str,
    table: str,
    nrows: Optional[int] = None,
    columns: Optional[Union[List[str], tuple]] = None,
    coerce_float: bool = True,
    date_cols: Optional[Union[List[str], Dict[str, str]]] = None,
    dtype_backend: str = "pyarrow",
) -> pd.DataFrame:
    """Download a WRDS table (or subset) as a DataFrame with dtype_backend support."""

    cols = "*" if columns is None else ", ".join(columns)
    limit = "" if nrows is None else f" LIMIT {nrows}"

    sql = f"SELECT {cols} FROM {library}.{table}{limit}"

    return run_wrds_query(
        sql_string=sql,
        coerce_float=coerce_float,
        date_cols=date_cols,
        dtype_backend=dtype_backend,
    )


def list_wrds_libraries(substr: Optional[str] = None) -> List[str]:
    """List available WRDS libraries, optionally filtering by substring."""
    with wrds_connection() as db:
        libs = db.list_libraries()
    if substr is not None:
        return [lib for lib in libs if substr in lib]
    return libs


def list_wrds_tables(library: str, substr: Optional[str] = None) -> List[str]:
    """List tables in a WRDS library, optionally filtering by substring."""
    with wrds_connection() as db:
        tables = db.list_tables(library=library)
    if substr is not None:
        return [t for t in tables if substr in t]
    return tables


def get_column_info(library: str, table: str) -> pd.DataFrame:
    """Retrieve column metadata for a WRDS table as a DataFrame."""
    with wrds_connection() as db:
        info = pd.DataFrame.from_dict(db.insp.get_columns(table, schema=library))
    return info[["name", "nullable", "type", "comment"]]
