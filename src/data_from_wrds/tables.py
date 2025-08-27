
LINK_TABLES = {
    "compustat_fundamentals_annual_ccm_linker": {
        "tables": ["comp.funda","crsp.ccmxpf_lnkhist"],
        "sql": """SELECT f.datadate, f.gvkey, f.iid, l.lpermno, l.lpermco  
                    FROM comp.funda as f
                    INNER JOIN crsp.ccmxpf_lnkhist  as l ON f.gvkey = l.gvkey 
                    WHERE datadate BETWEEN linkdt AND COALESCE(linkenddt, CURRENT_DATE)
                            AND linktype IN ('LU','LC') 
                            AND indfmt='INDL' AND datafmt='STD' AND popsrc='D' AND consol='C'
                """
    },
}

DATA_TABLES = {

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

    "crsp_monthly_stock_file": {
        "library": "crsp", "table": "msf", 
        "grain": {"firm":"permco","issue":"permno","date":"date","freq":"M"}
    },
    "crsp_daily_stock_file": {
        "library": "crsp", "table": "dsf", 
        "grain": {"firm":"permco","issue":"permno","date":"date","freq":"D"}
    },

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
    
    "worldscope_fundamentals_annual": {
        "library": "trws", "table": "wrds_ws_funda", 
        "grain": {"firm":"item6105","actual_restated":"freq","date":"item5350","freq":"A"}
    },    
    "worldscope_fundamentals_quarterly": {
        "library": "trws", "table": "wrds_ws_fundq", 
        "grain": {"firm":"item6105","actual_restated":"freq","date":"item5350","freq":"Q"}
    }

}
