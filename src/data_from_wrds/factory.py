from .fetch_tools import get_wrds_table, get_column_info
from .tables import LINK_TABLES, DATA_TABLES

def make_data_table_fetcher(func_name:str, library: str, table:str):
    def fetcher():
        return get_wrds_table(library=library, table=table)
    fetcher.__name__= func_name
    return fetcher 

def make_all_data_table_fetchers(data_tables: dict, module_globals: dict):
    exports = []
    for func_name, spec in data_tables.items():
        f = make_data_table_fetcher(func_name=func_name, library=spec['library'], table=spec['table'])
        module_globals[func_name] = f 
        exports.append(func_name)
    module_globals["__all__"] = tuple(exports )

make_all_data_table_fetchers(DATA_TABLES, globals())
