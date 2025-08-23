import pandas as pd 
from .fetch_tools import get_wrds_table, get_column_info

def ccm_linker(columns: list=None, nrows: int=None) -> pd.DataFrame:
    return get_wrds_table(library='crsp', table='ccmxpf_lnkhist', columns=columns, nrows=nrows)

def ccm_linker_meta():
    return get_column_info(library='crsp',table='ccmxpf_lnkhist').assign(schema='crsp', table='ccmxpf_lnkhist')
