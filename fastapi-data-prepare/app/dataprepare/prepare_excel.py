import re
import copy
from fastapi import FastAPI
import pandas as pd
import numpy as np
from io import BytesIO

from .config import *
from app.dataprepare.helpers import *



def get_headers(_file, config_dict):
    hs_index = int(config_dict.get("h_start_index"))
    he_index = int(config_dict.get("h_end_index"))
    excel_sheet_num = int(config_dict.get("excel_sheet_num"))

    pd_sheet_name = excel_sheet_num - 1
    start_index = hs_index - 1
    end_index = he_index - 1

    df = pd.read_excel(_file, header=None, sheet_name=pd_sheet_name)

    dfs = df.loc[start_index:end_index]
    dfs = dfs.replace(np.NaN, '')

    l_d = dfs.values.tolist()

    # replace '' with index -1 value
    for i in range(len(l_d)):
        item_list = l_d[i]
        for j in range(len(item_list)):
            item = str(item_list[j])
            if len(item) == 0:
                if j > 0:
                    l_d[i][j] = l_d[i][j-1]

    # concat rows to columns  name
    r_list = []
    col_num = 0
    for i in zip(*l_d):
        r_str = ""
        col_num += 1
        for j in i:
            j = str(j)
            if len(j):
                if len(r_str):
                    r_str += "."+j
                else:
                    r_str += j
        if r_str == "":
            r_str = f"col_{col_num}"
        r_str = r_str.replace("(", "")
        r_str = r_str.replace(")", "")
        r_str = r_str.replace("[", "")
        r_str = r_str.replace("]", "")


        r_list.append(r_str)

    for i in range(len(r_list)) :
        r_list[i] = cut_by_bytes_size(r_list[i])
    
    return r_list


def excel_prepare_df(_file, _param, logger=None, is_url=False):

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

    hs_index = int(_param.get("h_start_index"))
    he_index = int(_param.get("h_end_index"))
    excel_sheet_num = int(_param.get("excel_sheet_num"))

    pd_sheet_name = excel_sheet_num - 1
    header_start_index = hs_index - 1
    header_end_index = he_index - 1

    h_file = copy.copy(_file)

    logger.info("Creating headers")
    header_names = get_headers(h_file, _param)

    logger.info("Convert file to dataframe")
    df = pd.read_excel(_file, header=None,
                       skiprows=header_end_index + 1,
                       sheet_name=pd_sheet_name)


    if len(header_names) == len(df.columns):
        header_names = rename_duplicate_headers(header_names)
        df.columns = header_names

    df = df_drop_xy_nan(df)
    df = df_nan_to_none(df)

    df = get_df_headers_string(df)
    return df
