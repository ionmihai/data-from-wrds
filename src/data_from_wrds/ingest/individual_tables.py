from functools import partial
from .fetch_tools import get_wrds_table, get_column_info

REGISTRY = {
# Compustat tables
    "compustat_fundamentals_annual": {
        "library": "comp", "table": "funda", 
        "grain": {"firm":"gvkey","issue":"iid","date":"datadate","freq":"A"}
    },
    "compustat_fundamentals_quarterly": {
        "library": "comp", "table": "fundq", 
        "grain": {"firm":"gvkey","issue":"iid","date":"datadate","freq":"Q"}
    },
    "compustat_company_static": {
        "library": "comp", "table": "company", 
        "grain": {"firm":"gvkey"}
    },
# CRSP tables
    "crsp_monthly_stock_file": {
        "library": "crsp", "table": "msf", 
        "grain": {"firm":"permco","issue":"permno","date":"date","freq":"M"}
    },
    "crsp_daily_stock_file": {
        "library": "crsp", "table": "dsf", 
        "grain": {"firm":"permco","issue":"permno","date":"date","freq":"D"}
    },
# Fama-French tables
    "fama_french_3factors_monthly": {
        "library": "ff", "table": "factors_monthly", 
        "grain": {"date":"date","freq":"M"}
    },
    "fama_french_3factors_daily": {
        "library": "ff", "table": "factors_daily", 
        "grain": {"date":"date","freq":"D"}
    },    
    "fama_french_5factors_monthly": {
        "library": "ff", "table": "fivefactors_monthly", 
        "grain": {"date":"date","freq":"M"}
    },
    "fama_french_5factors_daily": {
        "library": "ff", "table": "fivefactors_daily", 
        "grain": {"date":"date","freq":"D"}
    } ,  
    "fama_french_portoflios_2x3": {
        "library": "ff", "table": "portfolios", 
        "grain": {"date":"date","freq":"M"}
    },
    "fama_french_portfolios_5x5": {
        "library": "ff", "table": "portfolios25", 
        "grain": {"date":"date","freq":"M"}
    }, 
# Worldscope tables
    "worldscope_fundamentals_annual": {
        "library": "trws", "table": "wrds_ws_funda", 
        "grain": {"firm":"item6105","actual_restated":"freq","date":"item5350","freq":"A"}
    },    
    "worldscope_fundamentals_quarterly": {
        "library": "trws", "table": "wrds_ws_fundq", 
        "grain": {"firm":"item6105","actual_restated":"freq","date":"item5350","freq":"Q"}
    }
}

def _make_fetch_function(func_name:str, library: str, table:str):
    """Builds function that gets the right wrds table allowing for all other params of `get_wrds_table`."""
    data_fetcher = partial(get_wrds_table, library=library, table=table)
    def meta_fetcher(): 
        return get_column_info(library=library, table=table).assign(library=library, table=table)
    #fetcher.__name__= func_name
    #meta.__name__ = f"{func_name}_meta"
    return data_fetcher, meta_fetcher 

def make_all_data_table_fetchers(data_tables: dict, module_globals: dict):
    """Builds all fetch functions specified in `tables.DATA_TABLES` and adds them to this module's globals."""
    exports = []
    for func_name, spec in data_tables.items():
        data_fetcher, meta_fetcher = _make_fetch_function(func_name=func_name, library=spec['library'], table=spec['table'])
        module_globals[func_name] = data_fetcher 
        module_globals[f"{func_name}_meta"] = meta_fetcher 
        exports.append(func_name)
        exports.append(f"{func_name}_meta")
    module_globals["__all__"] = tuple(exports)

make_all_data_table_fetchers(REGISTRY, globals())
