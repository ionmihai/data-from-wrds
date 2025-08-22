import os 
from typing import Sequence
import pandas as pd 
import wrds 

def load_wrds_username() -> str:
    wrds_username = os.getenv("WRDS_USERNAME")
    if not wrds_username: raise RuntimeError("WRDS_USERNAME not found")
    return wrds_username

def load_wrds_password() -> str:
    wrds_password = os.getenv("WRDS_PASSWORD")
    if not wrds_password: raise RuntimeError("WRDS_PASSWORD not found")
    return wrds_password

def run_wrds_query(
    sql_string: str=None, # e.g. "SELECT * from ff.factors_monthly"
    params: Sequence=None, # Params cited in the `sql_string`
    coerce_float: bool=True, 
    date_cols: list|dict=None,
    chunksize: int=500000,
    dtype_backend: str="pyarrow",    #defaul it "numpy_nullable" in wrds package
) -> pd.DataFrame:
    """Downloads data from WRDS using the given PostgreSQL `sql_string`.
    This is just a wrapper around wrds.Connection().raw_sql()
    """

    try:
        db = wrds.Connection(wrds_username=load_wrds_username(), wrds_password=load_wrds_password())
        df = db.raw_sql(sql=sql_string, params=params, 
                        coerce_float=coerce_float, date_cols=date_cols, chunksize=chunksize, dtype_backend=dtype_backend)
    except Exception as err:
        raise err 
    finally:
        db.close()

    return df


def get_wrds_table(
    library: str,  # wrds Postgres schema (e.g. "crsp", "comp")
    table: str,    # table 
    nrows: int=None,       # None means get all rows
    columns: list|tuple=None,
    coerce_float: bool=True,
    date_cols: list|dict=None,    
) -> pd.DataFrame:

    if nrows is None: nrows = -1 

    try:
        db = wrds.Connection(wrds_username=load_wrds_username(), wrds_password=load_wrds_password())
        df = db.get_table(library, table, rows=nrows, columns=columns, coerce_float=coerce_float, date_cols=date_cols)
    except Exception as err:
        raise err 
    finally:
        db.close()

    return df

def list_wrds_libraries(substr: str=None) -> list:
    try:
        db = wrds.Connection(wrds_username=load_wrds_username(), wrds_password=load_wrds_password())
        libs = db.list_libraries()
    except Exception as err:
        raise err 
    finally:
        db.close()

    if substr is not None: return [libname for libname in libs if substr in libname]
    return libs

def list_wrds_tables(library: str, substr: str=None) -> list:
    try:
        db = wrds.Connection(wrds_username=load_wrds_username(), wrds_password=load_wrds_password())
        tables = db.list_tables(library=library)
    except Exception as err:
        raise err 
    finally:
        db.close()

    if substr is not None: return [tbl for tbl in tables if substr in tbl]
    return tables 

def get_column_info(library: str, table: str) -> list: 
    try:
        db = wrds.Connection(wrds_username=load_wrds_username(), wrds_password=load_wrds_password())
        table_info = pd.DataFrame.from_dict(
            db.insp.get_columns(table, schema=library)
        )
    except Exception as err:
        raise err 
    finally:
        db.close()    

    return table_info[["name", "nullable", "type", "comment"]]
    