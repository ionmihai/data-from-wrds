import pandas as pd 
from .fetch_tools import run_wrds_query, get_wrds_table, get_column_info


def stock_file(columns: list=None, nrows: int=None) -> pd.DataFrame:
    return get_wrds_table(library='crsp', table='msf', columns=columns, nrows=nrows)

def stock_file_meta() -> pd.DataFrame:
    return get_column_info(library='crsp', table='msf').assign(schema='crsp', table='msf')


def names_file(columns: list=None, nrows: int=None) -> pd.DataFrame:
    return get_wrds_table(library='crsp', table='msenames', nrows=nrows)

def names_file_meta() -> pd.DataFrame:
    return get_column_info(library='crsp', table='msenames').assign(schema='crsp', table='msenames')


def delist_file(columns: list=None, nrows: int=None) -> pd.DataFrame:
    return get_wrds_table(library='crsp', table='msedelist', nrows=nrows)

def delist_file_meta() -> pd.DataFrame:
    return get_column_info(library='crsp', table='msedelist').assign(schema='crsp', table='msedelist')


def stock_names_delist_merged(
    columns: list=None, #must include table names e.g ['msf.permno', 'msenames.ticker', 'msedelist.dlret']
    nrows: int=None,
    start_date: str=None, # Start date in MM/DD/YYYY format
    end_date: str=None #End date in MM/DD/YYYY format    
) -> pd.DataFrame:

    if columns is None: columns = '*'
    else: columns = ','.join(columns)

    sql_string = f"""SELECT {columns} 
                        FROM crsp.msf  
                        LEFT JOIN crsp.msenames 
                            ON crsp.msf.permno=crsp.msenames.permno 
                                AND crsp.msenames.namedt<=crsp.msf.date 
                                AND crsp.msf.date<=crsp.msenames.nameendt 
                        LEFT JOIN crsp.msedelist
                            ON crsp.msf.permno=crsp.msedelist.permno 
                            AND date_trunc('month', crsp.msf.date) = date_trunc('month', crsp.msedelist.dlstdt)
                        WHERE 1=1
                """
    if start_date is not None: sql_string += f" AND crsp.msf.date >= '{start_date}'"
    if end_date is not None: sql_string += f" AND crsp.msf.date <= '{end_date}'"  
    if nrows is not None: sql_string += f" LIMIT {nrows}"            
    
    df = run_wrds_query(sql_string)
    df = df.loc[:,~df.columns.duplicated()] 
    return df 

def stock_names_delist_merged_meta() -> pd.DataFrame:
    df = pd.concat([stock_file_meta(), names_file_meta(), delist_file_meta()], 
                    axis=0, ignore_index=True)
    return df.loc[~df['name'].duplicated(),:] 


