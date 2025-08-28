from typing import Literal
import pandas as pd
from .fetch_tools import get_column_info, get_wrds_table, run_wrds_query

DOCS = "https://wrds-www.wharton.upenn.edu/pages/support/manuals-and-overviews/lseg/worldscope/wrds-overview-worldscope/"

def fundamentals_annual(columns: list=None, nrows: int=None) -> pd.DataFrame:
    return get_wrds_table(library='trws', table='wrds_ws_funda', columns=columns, nrows=nrows)

def fundamentals_annual_meta() -> pd.DataFrame:
    return get_column_info(library='trws', table='wrds_ws_funda').assign(library='trws', table='wrds_ws_funda')


def company_file(columns: list=None, nrows: int=None) -> pd.DataFrame:
    return get_wrds_table(library='trws', table='wrds_ws_company', columns=columns, nrows=nrows)

def company_file_meta() -> pd.DataFrame:
    return get_column_info(library='trws', table='wrds_ws_company').assign(library='trws', table='wrds_ws_company')


def extended_filter(
    exclude_usa: bool=True, #WE EXCLUDE USA BY DEFAULT
    entity_type: Literal['A','C','E','F','G','I','S']='C', #ARD, Company, Exchange rate, Country average, Industry average, Index, Security 
    columns: list=None, #must contain table names eg ['wrds_ws_funda.freq','wrds_ws_company.item6026',]
    nrows: int=None,
    nation: str=None, #country name
    start_date: str=None, # Start date in MM/DD/YYYY format
    end_date: str=None #End date in MM/DD/YYYY format      
) -> pd.DataFrame:

    columns = '*' if columns is None else ','.join(columns)
    if nation is not None: nation = nation.upper()

    sql_string = f""" SELECT {columns}
                        FROM trws.wrds_ws_funda 
                        LEFT JOIN trws.wrds_ws_company 
                                ON trws.wrds_ws_funda.item6105 = trws.wrds_ws_company.item6105
                        WHERE trws.wrds_ws_funda.freq='A'
                                AND trws.wrds_ws_company.item6100='{entity_type}'
    """
    if nation is not None: sql_string += f" AND trws.wrds_ws_company.item6026='{nation}'"
    if exclude_usa: sql_string += " AND trws.wrds_ws_company.item6026!='UNITED STATES'"
    if start_date is not None: sql_string += f" AND trws.wrds_ws_funda.item5350 >= '{start_date}'"
    if end_date is not None: sql_string += f" AND trws.wrds_ws_funda.item5350 <= '{end_date}'"  
    if nrows is not None: sql_string += f" LIMIT {nrows}"

    df = run_wrds_query(sql_string)
    df = df.loc[:,~df.columns.duplicated()] 
    return df 
