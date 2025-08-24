import pandas as pd 
from .fetch_tools import run_wrds_query, get_wrds_table, get_column_info


def stock_file(columns: list=None, nrows: int=None) -> pd.DataFrame:
    return get_wrds_table(library='crsp', table='dsf', columns=columns, nrows=nrows)

def stock_file_meta() -> pd.DataFrame:
    return get_column_info(library='crsp', table='dsf').assign(schema='crsp', table='dsf')


def names_file(columns: list=None, nrows: int=None) -> pd.DataFrame:
    return get_wrds_table(library='crsp', table='dsenames', nrows=nrows)

def names_file_meta() -> pd.DataFrame:
    return get_column_info(library='crsp', table='dsenames').assign(schema='crsp', table='dsenames')


def delist_file(columns: list=None, nrows: int=None) -> pd.DataFrame:
    return get_wrds_table(library='crsp', table='dsedelist', nrows=nrows)

def delist_file_meta() -> pd.DataFrame:
    return get_column_info(library='crsp', table='dsedelist').assign(schema='crsp', table='dsedelist')


def stock_names_delist_merged(
    columns: list=None, #must include table names e.g ['dsf.permno', 'dsenames.ticker', 'dsedelist.dlret']
    nrows: int=None,
    start_date: str=None, # Start date in MM/DD/YYYY format
    end_date: str=None #End date in MM/DD/YYYY format    
) -> pd.DataFrame:

    if columns is None: columns = '*'
    else: columns = ','.join(columns)

    sql_string = f"""SELECT {columns} 
                        FROM crsp.dsf  
                        LEFT JOIN crsp.dsenames 
                            ON crsp.dsf.permno=crsp.dsenames.permno 
                                AND crsp.dsenames.namedt<=crsp.dsf.date 
                                AND crsp.dsf.date<=crsp.dsenames.nameendt 
                        LEFT JOIN crsp.dsedelist
                            ON crsp.dsf.permno=crsp.dsedelist.permno 
                            AND date_trunc('month', crsp.dsf.date) = date_trunc('month', crsp.dsedelist.dlstdt)
                        WHERE 1=1
                """
    if start_date is not None: sql_string += f" AND crsp.dsf.date >= '{start_date}'"
    if end_date is not None: sql_string += f" AND crsp.dsf.date <= '{end_date}'"  
    if nrows is not None: sql_string += f" LIMIT {nrows}"            
    
    df = run_wrds_query(sql_string)
    df = df.loc[:,~df.columns.duplicated()] 
    return df 

def stock_names_delist_merged_meta() -> pd.DataFrame:
    df = pd.concat([stock_file_meta(), names_file_meta(), delist_file_meta()], 
                    axis=0, ignore_index=True)
    return df.loc[~df['name'].duplicated(),:] 


