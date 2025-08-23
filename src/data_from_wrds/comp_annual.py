import pandas as pd 
from .fetch_tools import run_wrds_query, get_wrds_table, get_column_info


def fundamentals_file(columns: list=None, nrows: int=None) -> pd.DataFrame:
    return get_wrds_table(library='comp', table='funda', columns=columns, nrows=nrows)

def fundamentals_file_meta():
    return get_column_info(library='comp',table='funda').assign(schema='comp', table='funda')


def company_file(columns: list=None, nrows: int=None) -> pd.DataFrame:
    return get_wrds_table(library='comp', table='company', columns=columns, nrows=nrows)

def company_file_meta():
    return get_column_info(library='comp',table='company').assign(schema='comp', table='company')


def ccm_linker(columns: list=None, nrows: int=None) -> pd.DataFrame:
    return get_wrds_table(library='crsp', table='ccmxpf_lnkhist', columns=columns, nrows=nrows)

def ccm_linker_meta():
    return get_column_info(library='crsp',table='ccmxpf_lnkhist').assign(schema='crsp', table='ccmxpf_lnkhist')


def crsp_compustat_merged_file(
    columns: list=None, #must include library andtable names e.g ['comp.funda.gvkey', 'comp.company.conm', 'crsp.ccmxpf_lnkhist.lpermno']
    nrows: int=None
) -> pd.DataFrame:

    if columns is None: columns = '*'
    else: columns = ','.join(columns)

    sql_string = f"""SELECT {columns} 
                    FROM comp.funda
                    LEFT JOIN comp.company 
                            ON comp.funda.gvkey = comp.company.gvkey 
                    INNER JOIN crsp.ccmxpf_lnkhist 
                            ON comp.funda.gvkey = crsp.ccmxpf_lnkhist.gvkey 
                    WHERE datadate BETWEEN crsp.ccmxpf_lnkhist.linkdt 
                            AND COALESCE(crsp.ccmxpf_lnkhist.linkenddt, CURRENT_DATE)
                            AND crsp.ccmxpf_lnkhist.linktype IN ('LU','LC') 
                            AND indfmt='INDL' AND datafmt='STD' AND popsrc='D' AND consol='C'
                """
    if nrows is not None: sql_string += f" LIMIT {nrows}"            
    
    df = run_wrds_query(sql_string)
    df = df.loc[:,~df.columns.duplicated()] 
    return df 

def crsp_compustat_merged_file_meta() -> pd.DataFrame:
    df = pd.concat([fundamentals_file_meta(), company_file_meta(), ccm_linker_meta()], 
                    axis=0, ignore_index=True)
    return df.loc[~df['name'].duplicated(),:] 
