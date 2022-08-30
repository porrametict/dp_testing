import requests
import re
import pandas as pd
import numpy as np
import math
from typing import Optional

from .config import *
from .type import get_data_type
from .thai_language_helper import parse_alien_language

POSTGRES_MAXIMUM_COLUMN_LENGTH = 60

def df_nan_to_none(df):
    df = df.where(pd.notnull(df), None)
    df = df.replace(np.NaN, None)
    df = df.replace({np.nan: None})

    return df


def df_drop_xy_nan(df):
    df = df.dropna(how="all", axis=0)
    df = df.dropna(how="all", axis=1)
    return df


def get_df_headers_string(df):
    _headers = df.columns.tolist()

    _new_headers = []

    for i in range(len(_headers)):
        item = str(_headers[i])

        if item.isdigit() or item == "None" or item == "nan" or not item:
            item = f"col_{i}"
        item = cut_by_bytes_size(item)
        _new_headers.append(item)

    df.columns = _new_headers
    return df


def http_request(url: str, method: str = "GET", headers: dict = dict(), json_body: dict = dict()) -> dict:

    try:
        if method == "GET":
            r = requests.get(url, headers=headers)
        else:
            r = requests.post(url, headers=headers, json=json_body)

        r.raise_for_status()

        if r.ok:
            return r

    except requests.exceptions.RequestException as e:
        raise e


def contain_in_content_type_list(content_type, content_type_list):
    for item in content_type_list:
        if item in content_type:
            return True
    return False


def get_format(content_type: str) -> Optional[str]:
    """
        คืนค่า format ของ content_type ที่ได้รับ
        โดย content_type จะต้อง ระบุข้อมูลใน body เป็น CSV หรือ JSON เท่านั้น อื่นๆ คืนค่า None

    Args:
        content_type (str): headers['content-type'] ของ response

    Returns:
        Optional[str]: รูปแบบของข้อมูลที่ได้จาก content-type 
    """

    if contain_in_content_type_list(content_type, JSON_CONTENT_TYPE_LIST):
        return JSON_FORMAT
    elif contain_in_content_type_list(content_type, CSV_CONTENT_TYPE_LIST):
        return CSV_FORMAT
    elif contain_in_content_type_list(content_type, XLSX_CONTENT_TYPE):
        return XLSX_FORMAT
    # elif contain_in_content_type_list(content_type, XLSB_CONTENT_TYPE):
    #     return XLSB_FORMAT
    # elif contain_in_content_type_list(content_type, XLSM_CONTENT_TYPE):
    #     return XLSM_FORMAT
    elif contain_in_content_type_list(content_type, XLS_CONTENT_TYPE):
        return XLS_FORMAT
    elif contain_in_content_type_list(content_type, TEXT_CONTENT_TYPE_LIST):
        return TEXT_FORMAT
    elif contain_in_content_type_list(content_type, APNG_CONTENT_TYPE):
        return APNG_FORMAT
    elif contain_in_content_type_list(content_type, AVIF_CONTENT_TYPE):
        return AVIF_FORMAT
    elif contain_in_content_type_list(content_type, GIF_CONTENT_TYPE):
        return GIF_FORMAT
    elif contain_in_content_type_list(content_type, JPEG_CONTENT_TYPE):
        return JPEG_FORMAT
    elif contain_in_content_type_list(content_type, PNG_CONTENT_TYPE):
        return PNG_FORMAT
    elif contain_in_content_type_list(content_type, SVG_CONTENT_TYPE):
        return SVG_FORMAT
    elif contain_in_content_type_list(content_type, WEBP_CONTENT_TYPE):
        return WEBP_FORMAT
    elif contain_in_content_type_list(content_type,XML_CONTENT_TYPE_LIST):
        return XML_FORMAT
    else:
        return None


def get_resource_show(resource_id, settings):
    ckan_sysadmin_api_key = settings.ckan_sysadmin_api_key
    ckan_resource_show_url = settings.ckan_resource_show_url
    http_header = {
        "Authorization":  ckan_sysadmin_api_key
    }
    params = {
        "id": resource_id
    }
    r = requests.get(f'{ckan_resource_show_url}',
                     headers=http_header, params=params)
    r.raise_for_status()
    return r.json()


def map_data_with_schema(df: pd.DataFrame, schema):

    delete_keys = []
    for i in schema:
        if SCHEMA_SELECTED_KEY not in i or not i[SCHEMA_SELECTED_KEY]:
            delete_keys.append(i[SCHEMA_FIELD_KEY])

    df = df.drop(columns=delete_keys)

    # rename key to new new_field_name
    num_fields_map = {}
    fields_map = {}
    for i in range(len(schema)):
        _field = schema[i][SCHEMA_FIELD_KEY]
        _new_field = schema[i][SCHEMA_NEW_FIELD_NAME] if SCHEMA_NEW_FIELD_NAME in schema[i].keys(
        ) and schema[i][SCHEMA_NEW_FIELD_NAME].strip() else ""
        if (_field not in delete_keys) and _new_field and _new_field != _field:
            if _field.isnumeric():
                num_fields_map[int(_field)] = _new_field
            else:
                fields_map[_field] = _new_field

    # rename field
    df = df.rename(columns=fields_map)

    df = df.rename(columns=num_fields_map)
    # just replace Nan to None
    df = df_nan_to_none(df)
    results = df.to_dict("records")

    return results


def get_frist_row(df_list: list, column_index: int):

    first_row = df_list[0][column_index]

    if first_row is None:  # try to get value
        for row in df_list:
            try : 
                fr_is_nan = math.isnan(row[column_index])
            except :
                fr_is_nan = False 
            if row[column_index] is not None and not  fr_is_nan:
                first_row = row[column_index]
                break
    return first_row


def get_columns(df: pd.DataFrame) -> list:

    columns = list()
    df = df_nan_to_none(df)
    column_names = df.columns.values.tolist()
    df_list = df.values.tolist()
    for column_name_index in range(df.shape[1]):
        first_row = get_frist_row(df_list, column_name_index)
        column_data = {
            SCHEMA_SELECTED_KEY: True,
            SCHEMA_FIELD_KEY: column_names[column_name_index],
            SCHEMA_TYPE_KEY:  get_data_type(first_row),
            SCHEMA_EXAMPLE_DATA_KEY: first_row,
            SCHEMA_DESCRIPTION_KEY: "",
        }
        columns.append(column_data)
    return columns


def dataframe_to_list(df: pd.DataFrame) -> list:
    return [df.columns.values.tolist()] + df.values.tolist()



def utf8len(s):
    try :
        return len(s.encode('utf-8'))
    except :
        return 4 # The maximum possible size is one character.
    
def cut_by_bytes_size (x_str,MAX_SIZE :int = POSTGRES_MAXIMUM_COLUMN_LENGTH):
    new_str  = ""
    curr_index = int(MAX_SIZE/4)
    while (True) :
        new_str  = x_str[:curr_index]
        curr_size = utf8len(new_str)
        if curr_size == MAX_SIZE  or len(new_str) == len(x_str): 
            break
        elif curr_size > MAX_SIZE :
            new_str = new_str[:-1]
            break
        curr_index += 1
    
    return new_str




def rename_duplicate_headers(h_list:list):
    for i in range(1, len(h_list)):
        count_dup = 0
        curr_item = str(h_list[i])

        for j in range(0, i):
            prev_item = str(h_list[j])
            qq_con = curr_item
            qq_con = qq_con.replace("-", "\-")
            qq_con = qq_con.replace(".", "\.")
            qq_con = qq_con.replace("_", "\_")

            is_duplicate = re.search(f"C*\d*\_*{qq_con}", prev_item)
            if is_duplicate:
                count_dup += 1
        if count_dup:
            h_list[i] = f'C{count_dup+1}_{curr_item}'
    return h_list