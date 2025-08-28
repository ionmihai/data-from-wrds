import pandas as pd 
from .fetch_tools import run_wrds_query, get_wrds_table, get_column_info

def stock_file(columns: list=None, nrows: int=None) -> pd.DataFrame:
    """Download the new CIZ version of the CRSP daily stock file. 
    It now contains the delisting variables and the header (names) variables.
    """
    return get_wrds_table(library='crsp', table='dsf_v2', columns=columns, nrows=nrows)

def stock_file_meta() -> pd.DataFrame:
    return get_column_info(library='crsp', table='dsf_v2').assign(schema='crsp', table='dsf_v2')

def extended_filter(
    columns: list=None,
    main_exchanges: bool=True,
    common_stock: bool=True,
    nrows: int=None,
    start_date: str=None, # Start date in MM/DD/YYYY format
    end_date: str=None #End date in MM/DD/YYYY format    
) -> pd.DataFrame:

    if columns is None: columns = '*'
    else: columns = ','.join(columns)

    sql_string = f"""SELECT {columns} 
                        FROM crsp.dsf_v2
                        WHERE 1=1
                """
    if main_exchanges: sql_string += " AND primaryexch IN ('N', 'A', 'Q') AND conditionaltype='RW' AND tradingstatusflg='A'"
    if common_stock: sql_string += """ AND sharetype='NS' AND securitytype='EQTY' AND securitysubtype='COM' 
                                        AND usincflg='Y' AND issuertype IN ('ACOR', 'CORP')"""
    if start_date is not None: sql_string += f" AND dlycaldt >= '{start_date}'"
    if end_date is not None: sql_string += f" AND dlycaldt <= '{end_date}'"  
    if nrows is not None: sql_string += f" LIMIT {nrows}"            
    
    df = run_wrds_query(sql_string)
    df = df.loc[:,~df.columns.duplicated()] 
    return df 
