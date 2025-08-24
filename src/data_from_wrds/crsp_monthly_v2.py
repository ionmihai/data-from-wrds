import pandas as pd 
from .fetch_tools import run_wrds_query, get_wrds_table, get_column_info

MIGRATION_DOCS = "https://wrds-www.wharton.upenn.edu/pages/support/manuals-and-overviews/crsp/stocks-and-indices/crsp-stock-and-indexes-version-2/crsp-ciz-faq/"

def migration_meta():
    return get_wrds_table(library='crsp', table='metaSiztoCiz')

def stock_file(columns: list=None, nrows: int=None) -> pd.DataFrame:
    return get_wrds_table(library='crsp', table='msf_v2', columns=columns, nrows=nrows)

def stock_file_meta() -> pd.DataFrame:
    return get_column_info(library='crsp', table='msf_v2').assign(schema='crsp', table='msf_v2')

def extended_filter(
    columns: list=None, #must include table names e.g ['msf.permno', 'msenames.ticker', 'msedelist.dlret']
    main_exchanges: bool=True,
    common_stock: bool=True,
    nrows: int=None,
    start_date: str=None, # Start date in MM/DD/YYYY format
    end_date: str=None #End date in MM/DD/YYYY format    
) -> pd.DataFrame:

    if columns is None: columns = '*'
    else: columns = ','.join(columns)

    sql_string = f"""SELECT {columns} 
                        FROM crsp.msf_v2 AS a
                        WHERE 1=1
                """
    if main_exchanges: sql_string += " AND primaryexch IN ('N', 'A', 'Q') AND conditionaltype='RW' AND tradingstatusflg='A'"
    if common_stock: sql_string += " AND sharetype='NS' AND securitytype='EQTY' AND securitysubtype='COM' AND usincflg='Y' AND issuertype IN ('ACOR', 'CORP')"
    if start_date is not None: sql_string += f" AND a.mthcaldt >= '{start_date}'"
    if end_date is not None: sql_string += f" AND a.mthcaldt <= '{end_date}'"  
    if nrows is not None: sql_string += f" LIMIT {nrows}"            
    
    df = run_wrds_query(sql_string)
    df = df.loc[:,~df.columns.duplicated()] 
    return df 

def filter_main_exchanges(df):
    """Equivalent to legacy exchcd = 1,2, or 3"""
    return df.loc[(df.primaryexch.isin(['N', 'A', 'Q'])) & 
                   (df.conditionaltype =='RW') & 
                   (df.tradingstatusflg =='A')].copy()


def filter_common_stock(df):
    """Equivalent to legacy shrcd = 10 or 11"""
    return df.loc[(df.sharetype=='NS') & 
                    (df.securitytype=='EQTY') & 
                    (df.securitysubtype=='COM') & 
                    (df.usincflg=='Y') & 
                    (df.issuertype.isin(['ACOR', 'CORP']))]