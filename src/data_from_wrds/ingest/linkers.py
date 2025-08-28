import pandas as pd 
from .fetch_tools import run_wrds_query, get_wrds_table, get_column_info

def ccm_linker(columns: list=None, nrows: int=None) -> pd.DataFrame:
    return get_wrds_table(library='crsp', table='ccmxpf_lnkhist', columns=columns, nrows=nrows)

def ccm_linker_meta():
    return get_column_info(library='crsp',table='ccmxpf_lnkhist').assign(schema='crsp', table='ccmxpf_lnkhist')


def ibestickers_w_permnos(nrows: int=None):
    return get_wrds_table(library='wrdsapps', table='ibcrsphist', nrows=nrows)

def ibestickers_w_permnos_meta():
    return get_column_info(library='wrdsapps', table='ibcrsphist').assign(schema='wrdsapps', table='ibcrsphist')


def bondcusips_w_permnos(nrows: int=None):
    return get_wrds_table(library='wrdsapps', table='bondcrsp_link', nrows=nrows)

def bondcusips_w_permnos_meta(nrows: int=None):
    return get_column_info(library='wrdsapps', table='bondcrsp_link').assign(schema='wrdsapps', table='bondcrsp_link')


def annual_gvkeys_w_permnos(
    unique_gvkey_datadate: bool=True, 
    nrows: int=None
) -> pd.DataFrame:

    sql_string="""SELECT a.datadate, a.gvkey , b.lpermno as permno, b.lpermco as permco, b.liid, b.linkprim
                    FROM comp.funda AS a
                    INNER JOIN crsp.ccmxpf_lnkhist AS b ON a.gvkey = b.gvkey
                    WHERE datadate BETWEEN b.linkdt AND COALESCE(b.linkenddt, CURRENT_DATE)
                            AND b.linktype IN ('LU','LC')
                            AND indfmt='INDL' AND datafmt='STD' AND popsrc='D' AND consol='C'
                """
    if unique_gvkey_datadate: sql_string += "AND b.linkprim IN ('P','C')"
    if nrows is not None: sql_string += f" LIMIT {nrows}"

    return run_wrds_query(sql_string=sql_string)


def quarterly_gvkeys_w_permnos(
    unique_gvkey_datadate: bool=True, 
    nrows: int=None
) -> pd.DataFrame:

    sql_string="""SELECT a.datadate, a.gvkey , b.lpermno as permno, b.lpermco as permco, b.liid, b.linkprim
                    FROM comp.fundq AS a
                    INNER JOIN crsp.ccmxpf_lnkhist AS b ON a.gvkey = b.gvkey
                    WHERE datadate BETWEEN b.linkdt AND COALESCE(b.linkenddt, CURRENT_DATE)
                            AND b.linktype IN ('LU','LC')
                            AND indfmt='INDL' AND datafmt='STD' AND popsrc='D' AND consol='C'
                """
    if unique_gvkey_datadate: sql_string += "AND b.linkprim IN ('P','C')"
    if nrows is not None: sql_string += f" LIMIT {nrows}"

    return run_wrds_query(sql_string=sql_string)

