import pandas as pd 
from .fetch_tools import run_wrds_query, get_wrds_table, get_column_info
from .linkers import ccm_linker_meta

def fundamentals_annual_ccm(
    unique_gvkey_datadate: bool=False, #set to false if you want identical results to WRDS CCM website
    columns: list=None, #must include table names e.g ['funda.gvkey', 'company.conm', 'ccmxpf_lnkhist.lpermno']
    nrows: int=None,
    start_date: str=None, # Start date in MM/DD/YYYY format
    end_date: str=None #End date in MM/DD/YYYY format    
) -> pd.DataFrame:
    """This function produce identical results to the ones we would obtain if we used the WRDS CCM website. 
    The website produces a small number of `gvkey-datadate` duplicates (about 1% of the data) because each `permno` maps to a unique `gvkey+iid` value
    and some gvkeys have multiple share classes (different iid's). 
    Set `unique_gvkey_datadate` to True to obtain unique `gvkey-datadate` records.
    """

    columns = '*' if columns is None else ','.join(columns)

    sql_string = f"""SELECT {columns} 
                    FROM comp.funda
                    LEFT JOIN comp.company ON comp.funda.gvkey = comp.company.gvkey 
                    INNER JOIN crsp.ccmxpf_lnkhist ON comp.funda.gvkey = crsp.ccmxpf_lnkhist.gvkey 
                    WHERE datadate BETWEEN crsp.ccmxpf_lnkhist.linkdt AND COALESCE(crsp.ccmxpf_lnkhist.linkenddt, CURRENT_DATE)
                            AND crsp.ccmxpf_lnkhist.linktype IN ('LU','LC') 
                            AND indfmt='INDL' AND datafmt='STD' AND popsrc='D' AND consol='C'
                """
    if unique_gvkey_datadate: sql_string += "AND crsp.ccmxpf_lnkhist.linkprim IN ('P','C')"
    if start_date is not None: sql_string += f" AND datadate >= '{start_date}'"
    if end_date is not None: sql_string += f" AND datadate <= '{end_date}'"    
    if nrows is not None: sql_string += f" LIMIT {nrows}"            
    
    df = run_wrds_query(sql_string)
    df = df.loc[:,~df.columns.duplicated()] 
    return df 

def crsp_compustat_merged_file_meta() -> pd.DataFrame:
    df = pd.concat([fundamentals_file_meta(), company_file_meta(), ccm_linker_meta()], 
                    axis=0, ignore_index=True)
    return df.loc[~df['name'].duplicated(),:] 
