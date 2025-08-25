from typing import Literal
import pandas as pd 
from .fetch_tools import get_wrds_table, get_column_info

def ff_factors(
    freq: Literal['monthly','daily']='monthly',
    five_factor_model: bool=False #by default, we get the three factor model
) -> pd.DataFrame:

    prefix = 'five' if five_factor_model else ""
    return get_wrds_table(library='ff', table=f"{prefix}factors_{freq}")

def ff_factors_meta(
    freq: Literal['monthly','daily']='monthly',
    five_factor_model: bool=False 
) -> pd.DataFrame:

    prefix = 'five' if five_factor_model else ""
    return get_column_info(library='ff', table=f"{prefix}factors_{freq}").assign(library='ff', table=f"{prefix}factors_{freq}")

def ff_portfolios(
    five_by_five: bool=False 
) -> pd.DataFrame:
    suffix = '25' if five_by_five else ''
    return get_wrds_table(library='ff', table=f"portfolios{suffix}")

def ff_portfolios_meta(
    five_by_five: bool=False 
) -> pd.DataFrame:
    suffix = '25' if five_by_five else ''
    return get_column_info(library='ff', table=f"portfolios{suffix}").assign(library='ff', table=f"portfolios{suffix}")