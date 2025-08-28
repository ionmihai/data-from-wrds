

REGISTRY = {
    "compustat_fundamentals_annual_ccm_linker": {
        "tables": ["comp.funda","crsp.ccmxpf_lnkhist"],
        "sql": """SELECT f.datadate, f.gvkey, f.iid, l.lpermno, l.lpermco, l.linkprim  
                    FROM comp.funda as f
                    INNER JOIN crsp.ccmxpf_lnkhist  as l ON f.gvkey = l.gvkey 
                    WHERE datadate BETWEEN linkdt AND COALESCE(linkenddt, CURRENT_DATE)
                            AND linktype IN ('LU','LC') 
                            AND indfmt='INDL' AND datafmt='STD' AND popsrc='D' AND consol='C'
                """
    },
}
