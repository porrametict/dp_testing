import pandas as pd
from io import BytesIO, StringIO


from .helpers import *
from .config import *


def get_pd_sep(delimeters, deliemter_other):
    pd_sep = ""
    delimeter_set = {
        "comma": ",",
        "tab": '\t',
        "semicolon": ';',
        "space": "\s+",
        "other": deliemter_other
    }
    for i in delimeters:
        tmp = delimeter_set.get(i)
        if len(pd_sep) > 1:
            pd_sep += "|"
            pd_sep += tmp
        else:
            pd_sep += tmp

    return pd_sep


def csv_prepare_df(_file, _param, logger=None, is_url=False, _file2=None):

    if is_url:
        _request_method = _param.get("request_method", "GET")
        _http_headers = _param.get("http_headers", {})
        _request_body = _param.get("request_body", {})

        logger.info("Fetching from: "+_file)
        r = http_request(_file,
                         _request_method,
                         _http_headers,
                         _request_body)
        _file = BytesIO(r.content)
        _file2 = StringIO(r.text)

    delimeters = _param.get("csv_delimeter")
    delimeter_other = _param.get("csv_delimeter_other")
    text_qualifier = _param.get("csv_text_qualifier")
    first_row_is_header = _param.get("csv_first_row_is_header")

    pd_header = 0 if first_row_is_header else None
    pd_sep = get_pd_sep(delimeters, delimeter_other)
    _kconfig = {
        "header": pd_header,
        "sep": pd_sep,
        "engine": "python",  # python can use regex delimeters
    }

    if text_qualifier and len(text_qualifier):
        _kconfig['quotechar'] = text_qualifier

    if pd_header is None:
        # for multiple delimeters must set headers
        pd_names = [str(i) for i in range(1, 200)]
        _kconfig['names'] = pd_names

    logger.info("Convert file to dataframe")
    try:
        df = pd.read_csv(_file, **_kconfig)
    except UnicodeDecodeError as e:
        if _file2:
            df = pd.read_csv(_file2, **_kconfig)
        else:
            raise e

    df = df_drop_xy_nan(df)
    df = df_nan_to_none(df)

    df = get_df_headers_string(df)
    df.columns = rename_duplicate_headers(df.columns.tolist())

    return df

