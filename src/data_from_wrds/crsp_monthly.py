import pandas as pd 
from .fetch_tools import run_wrds_query, get_wrds_table, get_column_info


def stock_file(columns: list=None, nrows: int=None) -> pd.DataFrame:
    return get_wrds_table(library='crsp', table='msf', columns=columns, nrows=nrows)

def stock_file_info() -> pd.DataFrame:
    return get_column_info(library='crsp', table='msf')


def names_file(columns: list=None, nrows: int=None) -> pd.DataFrame:
    return get_wrds_table(library='crsp', table='msenames', nrows=nrows)

def names_file_info() -> pd.DataFrame:
    return get_column_info(library='crsp', table='msenames')


def delist_file(columns: list=None, nrows: int=None) -> pd.DataFrame:
    return get_wrds_table(library='crsp', table='msedelist', nrows=nrows)

def delist_file_info() -> pd.DataFrame:
    return get_column_info(library='crsp', table='msedelist')


def full_feed(nrows: int=None) -> pd.DataFrame:

    sql_string = """SELECT * 
                        FROM crsp.msf AS a 
                        LEFT JOIN crsp.msenames AS b
                            ON a.permno=b.permno AND b.namedt<=a.date AND a.date<=b.nameendt 
                        LEFT JOIN crsp.msedelist as c
                            ON a.permno=c.permno AND date_trunc('month', a.date) = date_trunc('month', c.dlstdt)
                """
    if nrows is not None: sql_string += f" LIMIT {nrows}"            
    df = run_wrds_query(sql_string)
    df = df.loc[:,~df.columns.duplicated()] 
    return df 

def full_feed_info() -> pd.DataFrame:
    df = pd.concat([stock_file_info(), names_file_info(), delist_file_info()], axis=0)
    return df.loc[~df['name'].duplicated(),:] 


