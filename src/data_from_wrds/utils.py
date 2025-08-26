from typing import Iterable, Optional, Sequence

def cols(xs: Optional[Iterable[str]]) -> str:
    return '*' if not xs else ','.join(xs)

def where(*conds: Optional[str]) -> str:
    c = [x for x in conds if x]
    return (' WHERE ' + ' AND '.join(c)) if c else ''

def daterange(col: str, start: Optional[str], end: Optional[str]) -> str:
    parts = []
    if start: parts.append(f"{col} >= '{start}'")
    if end:   parts.append(f"{col} <= '{end}'")
    return ' AND '.join(parts) if parts else ''

def limit(n: Optional[int]) -> str:
    return f" LIMIT {n}" if n else ''

def one_of(val: str, allowed: Sequence[str]) -> str:
    if val not in allowed: raise ValueError(f"{val} not in {allowed}")
    return val

def prefix_cols(table_or_alias: str, cols: Iterable[str]) -> list[str]:
    return [f"{table_or_alias}.{c}" for c in cols]

def make_meta(schema: str, table: str):
    from .fetch_tools import get_column_info
    def _meta():
        return get_column_info(library=schema, table=table).assign(schema=schema, table=table)
    return _meta
