from generic_templates.report import Report
import pandas as pd
import numpy as np
from typing import List, Union


def column_threshold(report : Report, col : str, thresh : dict = None):
    if thresh is None:
        thresh = {
            'default': 1e-6,
            'CONDUCTOR_CD_CURREP_des_life_adjusted': 1e-3,
            'CONDUCTOR_CD_des_life_adjusted': 1e-3,
            'CONDUCTOR_CD_FCREP_des_life_adjusted': 1e-3
        }

    if col in thresh:
        report.print(f" - threshold for {col}: {thresh[col]}")
        return thresh[col]
    return thresh['default']

def drop_cols(df, columns):
    columns = set(columns) & set(df.columns)
    return df.drop(columns, axis=1)

def cmp_values(report, df1name, df2name, df1, df2, c, thresh, opt_checktype = True):
    if len(df1.index) == 0 or len(df2.index) == 0:
        report.print(" - no common rows")
        return []

    rowset = set()
    if opt_checktype:
        dt1 = str(df1[c].dtype)
        dt2 = str(df2[c].dtype)
        if dt1 == 'object':
            dt1 = ",".join(sorted(df1[c].apply(type).unique().astype(str)))
        
        if dt2 == 'object':
            dt2 = ",".join(sorted(df2[c].apply(type).unique().astype(str)))

        if dt1 != dt2:
            report.print(f" - {df1name} Type: {dt1}")
            report.print(f" - {df2name} Type: {dt2}")

    z2 = df1[c].isna() ^ df2[c].isna()
    if z2.sum()>0:
        report.print(f" - {df1name} has {df1[c].isna().sum()} NaNs")
        report.print(f" - {df2name} has {df2[c].isna().sum()} NaNs")
        report.print(f" - NaN/value mismatch in {z2.sum()} rows")
        report.attach(f"nan_value_{c}", z2)
        rowset |= set(list(df1[z2].index))

    try:
        zz = df1[c].astype(float) - df2[c].astype(float)
        epsilon = column_threshold(report, c, thresh)
        zzindex = sorted(list(df1[zz.abs()>epsilon].index))
        zzcount = len(zzindex)
        delta = zz.abs().max()
        if np.isnan(delta) or delta > epsilon:
            report.print(f" - {zzcount} rows over threshold")
            report.print(f" - max difference = {delta}")
            report.attach(f"delta_{c}", zzindex)
            rowset |= set(zzindex)
    except:
        nv = '<null value placeholder; should not appear in data>'
        zz = (df1[c].fillna(nv).astype(str) != df2[c].fillna(nv).astype(str))
        zzindex = sorted(list(df1[zz].index))
        zzcount = len(zzindex)
        if zzcount > 0:
            report.print(f" - {zzcount} rows have different values")
            report.attach(f'{c}_index', zz)
            rowset |= set(list(df1[zz].index))
    return rowset


def cmp_table(report:Report=None, thresh=None, ignore_cols=[], opt_checktype=True, **tables):
    """Compares two tables showing high level information

    Usage:
    cmp_table(SOMEDATA=df1, MOREDATA=df2)
    """

    autoprint = False
    if report is None:
        autoprint = True
        report = Report()
    
    df1name, df2name = tables.keys()

    df1, df2 = tables.values()
    df1 = drop_cols(df1, ignore_cols)
    df2 = drop_cols(df2, ignore_cols)

    df1c = set(df1.columns)
    df2c = set(df2.columns)
    colstate = False
    report.sub_heading("Columns")
    if len(df1c-df2c) > 0:
        colstate = True
        extra_cols = list(df1c-df2c)
        report.print(f"Columns only in {df1name}: ", extra_cols)
        report.attach(f"{df1name}cols", extra_cols)
    if len(df2c-df1c) > 0:
        colstate = True
        extra_cols = list(df2c-df1c)
        report.print(f"Columns only in {df2name}: ", extra_cols)
        report.attach(f"{df2name}cols", extra_cols)
    if not colstate:
        report.print(f"Columns in both tables are matched")
    common_cols = df1c & df2c
    report.sub_heading("Rows")
    df1i = set(df1.index)
    df2i = set(df2.index)
    rowstate = False
    if len(df1i)!=len(df2i):
        rowstate = True
        delta = len(df1i)-len(df2i)
        if delta > 0:
            report.print(f"{df2name} has {delta} fewer rows than {df1name}")
        else:
            report.print(f"{df1name} has {-delta} fewer rows than {df2name}")
    if len(df1i-df2i) > 0:
        rowstate = True
        extra_rows = sorted(list(df1i-df2i))
        report.print(f"{df1name} exclusive rows: {extra_rows}")
        report.attach(f"{df1name}rows", extra_rows)
    if len(df2i-df1i) > 0:
        rowstate = True
        extra_rows = sorted(list(df2i-df1i))
        report.print(f"{df2name} exclusive rows: {extra_rows}")
        report.attach(f"{df2name}rows", extra_rows)
    if not rowstate:
        report.print(f"Rows in both tables are matched")  

    olen1 = len(df1)
    olen2 = len(df2)
    df1 = df1[~df1.index.duplicated(keep='first')].copy()
    df2 = df2[~df2.index.duplicated(keep='first')].copy()
    if olen1 != len(df1):
        report.print(f"{df1name} has {olen1-len(df1)} duplicated rows by index")
    if olen2 != len(df2):
        report.print(f"{df2name} has {olen2-len(df2)} duplicated rows by index")
        
    df1 = df1.loc[sorted(list(df1i&df2i))]
    df2 = df2.loc[sorted(list(df1i&df2i))]
    common_rows_count = len(df1.index)
    report.print(f"{df1name} and {df2name} have {common_rows_count} rows in common")
    report.section("Data Comparison")
    rowset = set()
    if common_rows_count > 0 and len(common_cols) > 0:
        for c in common_cols:
            report.sub_section(f"Column {c}")
            rowset |= cmp_values(report, df1name, df2name, df1, df2, c, thresh, opt_checktype)
    else:
        if common_rows_count == 0:
            report.print("There are no rows in common per the index.  Nothing to compare.")
        if len(common_cols) == 0:
            report.print("There are no columns in common by column name.  Nothing to compare.")
    report.section("Overview")
    report.sub_section("Data Summary")
    report.print(f"These columns were ignored: {ignore_cols}")
    report.print(f"{len(rowset)} rows differ out of {len(df1i&df2i)} rows in both {df1name} and {df2name}")
    report.attach("delta_rowset", sorted(list(rowset)))
    if autoprint: print(report)
    return report
            
def cmp_col(column, report:Report=None, brief=True, thresh=None, **tables):
    """
    Compares a specific column across two tables.

    Usage:
        delta_df = cmp_col('MYCOLUMN', ADATAFRAME=df1, ANOTHERDF=df2)
        print(delta_df)
    """
    autoprint = False
    if report is None:
        autoprint = True
        report = Report()
    suffix = "."+column
    if brief:
        suffix=""
    df1name, df2name = tables.keys()
    df1, df2 = tables.values()
    df1c = set(df1.columns)
    df2c = set(df2.columns)
    common = df1c & df2c
    result = pd.DataFrame()
    if column in common:
        filt = None
        c = column
        dt1 = df1[c].dtype
        dt2 = df2[c].dtype
        if dt1 != dt2:
            report.print(f" - {df1name}.dtype={dt1}, {df2name}.dtype={dt2}")

        result[df1name+suffix] = df1[column]
        result[df2name+suffix] = df2[column]

        if str(dt1) == 'float64' or str(dt1) == 'int64':
            zz = (df1[c].astype(float) - df2[c].astype(float)).abs()
            report.print(f" - max difference = {zz.max()}")
            result['delta'] = zz
            epsilon = column_threshold(report, column, thresh)
            filt = (zz > epsilon)
        elif str(dt1) == 'object' or str(dt2) == 'object':
            filt = (df1[c].astype(str) != df2[c].astype(str))
            count = filt.sum()
            if count > 0:
                report.print(f" - {count} rows differ")
        else:
            report.print(f" - Neither Column types {str(dt1)} nor {str(dt2)} are implemented")

        if not filt is None and filt.sum()>0:
            result[f'changed'] = filt
            report.attach(f'{c}_changed', sorted(list(df1[filt].index)))
        report.attach('delta', result)
    else:
        if not column in df1.columns:
            report.print(f" - {df1name} has no column named {column}")
        if not column in df2.columns:
            report.print(f" - {df2name} has no column named {column}")
    if autoprint: print(report)
    return report
        
def cmp_row(id, report=None, **tables):
    """Compares rows from different tables

    Usage:
        cmp_row(id, NAME1=df1, NAME2=df2, ...)
    """
    autoprint = False
    if report is None:
        autoprint = True
        report = Report()
    cols=[]
    for k in tables:
        for c in tables[k].columns:
            if not c in cols: cols.append(c)

    qq = pd.DataFrame(index=cols)
    qq.index.name = id
    for k in tables:
        qq[k] = tables[k].loc[id].T
        for colname in qq.index:
            if not colname in tables[k].columns:
                qq.loc[colname,k] = '[Undefined]'

    if len(tables.keys())>1:
        qq['diff'] = (qq.iloc[:,0].astype(str) != qq.iloc[:,-1].astype(str)).replace(False,"").replace(True, "** CHANGED **")
    

    report.attach('table', qq)
    if autoprint:
        print(report)
    return report


def cmp_row_r(id, **tables):
    return cmp_row(id, report=None, **tables).attachment('table')

def disp_row(df):
    from IPython.display import display
    with pd.option_context('display.max_rows', 200):
        display(df.T)

def colnames_toupper(df, inplace = False):
    """
    Renames all columns to upper case
    """
    cols = list(df.columns)
    return df.rename(columns={c : c.upper() for c in cols}, inplace = inplace)

def fix_column_names(df, rename_list, inplace = False):
    """
    Renames all columns to upper case and then replaces special mixed case columns with 
    correct case from 'rename_list'
    """
    if not inplace:
        df = df.copy()

    colnames_toupper(df, True)
    renames = {}
    for k in rename_list.keys():
        renames[k.upper()] = rename_list[k]
    df.rename(columns=renames, inplace = True)
    return df

def setindex(df : pd.DataFrame, col : str):
    """ Sets the specified column as the table index, converting the column to integer first (if possible).
    Returns the reindexed dataframe.
    """
    try:
        df['id'] = df[col].astype(int)
    except:
        df['id'] = df[col]
    df = df.set_index('id')
    return df

def load_data(path : str, index_col = None, tolower = False, primary_key = None) -> pd.DataFrame :
    """ Loads data from a CSV or Parquet file
    
    Parameters:
      path : the file path to load from
      index_col : defaults to 0 to read the pandas default.  Set to None for files without index columns.
      tolower : if True, converts all columns to lower case after loading
      primary_key : use the specified column as the primary key, or by default use 'sap_equip_id'

    Returns dataframe

    """
    if path.endswith(".csv"):
        if index_col is None:
            df = pd.read_csv(path)
        else:
            df = pd.read_csv(path, index_col=index_col)
    else:
        df = pd.read_parquet(path)

    if tolower:
        df.rename(columns={x:x.lower() for x in df.columns}, inplace=True)

    if primary_key is None:
        if 'sap_equip_id' in df.columns:
            df = setindex(df, 'sap_equip_id')
        elif 'SAP_EQUIP_ID' in df.columns:
            df = setindex(df, 'SAP_EQUIP_ID')
        else:
            print("primary key not set")
    else:
        df = setindex(df, primary_key)
    return df
    
def common_index(df1, df2):
    """ Finds the set intersection of the indexes of two dataframes
    
    Parameters:
       df1 : a dataframe or dataframe index (or other iterable list of index values)
       df2 : the same but for a second dataframe
       
    Returns the intersection as a sorted list
    """
    if type(df1) is pd.DataFrame:
        df1 = df1.index
    if type(df2) is pd.DataFrame:
        df2 = df2.index
    q1 = set(df1)
    q2 = set(df2)
    return sorted(list(q1&q2))

def filter_changes(df_PREV : pd.DataFrame, df_CURR : pd.DataFrame, columns : List[str]) -> set:
    """ Returns a filter that is True for every column that has a change in the listed columns
    between two DataFrames that share a common index.  Returns False for every row that has 
    no columns that differ.

    Parameters:
        df_PREV : a dataset to compare with
        df_CURR : a dataset to be compared
        columns : a list of columns to check for differences

    Returns a filter
    """
    
    # assume no difference between df_PREV and df_CURR
    result_filter = pd.Series(False, index=list(df_PREV.index)).sort_index()
    
    for c in columns:
        print("column:", c)
        dtype = str(df_PREV[c].dtype)
        if dtype == 'float64':
            filt = (df_PREV[c]-df_CURR[c]).abs()
            result_filter = result_filter | (filt>1e-6)
        elif dtype == 'int64':
            filt = (df_PREV[c]-df_CURR[c]).abs()
            result_filter = result_filter | (filt>0)
        elif dtype == 'timedelta64[ns]':
            filt = (df_PREV[c]-df_CURR[c]).abs()
            result_filter = result_filter | (filt>pd.Timedelta(seconds=0))
        elif dtype == 'object':
            result_filter = result_filter | str(df_PREV[c]) != str(df_CURR[c])
        else:
            raise NotImplementedError(f"filter column type {filt.dtype}")

    for c in columns:
        result_filter = result_filter | (df_PREV[c].isna() ^ df_CURR[c].isna())
    return result_filter

def filter_elide(df : pd.DataFrame, index_list: List[int]) -> pd.DataFrame:
    ndx = sorted(list(set(df.index)-set(index_list)))
    return df.loc[ndx]

def select_etl(df : pd.DataFrame, sap_func_loc_no: str) -> pd.DataFrame:
    res = df[ (df.SAP_FUNC_LOC_NO == sap_func_loc_no) | 
              (df.GUEST_ETL1 == sap_func_loc_no) |
              (df.GUEST_ETL2 == sap_func_loc_no) | 
              (df.GUEST_ETL3 == sap_func_loc_no)]
    return res

def pp(ll, items_per_line=10):
    def sfmt(itm):
        if type(itm) is str:
            return "'%s'"%itm
        else: 
            return str(itm)
    if type(ll) is set:
        ll = sorted(list(ll))
    max = len(ll)
    if max == 0:
        out = "[]"
    elif max == 1:
        out = "[ " + sfmt(ll[0]) + ", ]"
    else:
        out = "[\n"
        for i in range(0, max, items_per_line):
            out += "    "

            out += ", ".join(map(sfmt,ll[i:i+items_per_line]))
            out += ",\n"
        out += "]"
    return out

def compare(index_col=None, ignore_cols=[], **kwargs):
    """
    Compares two pandas dataframes

    index_col : str - The name of the column to join on
    ignore_cols : List[str] - a list of columns to ignore for the comparison
    kwargs : two pandas dataframes (and the names to use to refer to them) for comparing

    Usage:
      compare(index_col = join_column_name, datasetname1 = df1, datasetname2 = df2)
    """
    if len(kwargs) != 2:
        raise Exception("Must pass in two dataframes to compare")
    argnames = list(kwargs)
    a = kwargs[argnames[0]]
    b = kwargs[argnames[1]]
        
    if not index_col is None:
        a = a.copy()
        a.index = a[index_col]
        b = b.copy()
        b.index = b[index_col]
    index_a_minus_b = set(a.index) - set(b.index)
    index_b_minus_a = set(b.index) - set(a.index)
    cols_a_minus_b = set(a.columns) - set(b.columns)
    cols_b_minus_a = set(b.columns) - set(a.columns)
    common_index = a.index.intersection(b.index)
    common_cols = a.columns.intersection(b.columns)
    a = a.loc[common_index, common_cols].copy().sort_index()
    a = a.loc[:,~a.columns.isin(ignore_cols)]
    #a.fillna(0, inplace=True)
    #return a
    b = b.loc[common_index, common_cols].copy().sort_index()
    b = b.loc[:,~b.columns.isin(ignore_cols)]
    #b.fillna(0, inplace=True)
    result = {
        "comparison": a.compare(b), 
        "index a not b": index_a_minus_b, 
        "index b not a": index_b_minus_a, 
        "cols a not b": cols_a_minus_b, 
        "cols b not a": cols_b_minus_a
    }
    print(f"Rows only in {argnames[0]}:", result["index a not b"])
    print(f"Rows only in {argnames[1]}:", result["index b not a"])
    print(f"Row count in common:", len(common_index))
    print(f"Columns only in {argnames[0]}:", result["cols a not b"])
    print(f"Columns only in {argnames[1]}:", result["cols b not a"])
    print(f"Column count in common:", len(common_cols))
    print(f"Ignored columns:", ignore_cols)
    return result['comparison']
    
