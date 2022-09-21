import pandas as pd
from pandas import json_normalize

from .config import *
from .type import get_data_type


def get_frist_row(df_list: list, column_index: int):

    first_row = df_list[0][column_index]

    if first_row is None:  # try to get value
        for row in df_list:
            if row[column_index] is not None:
                first_row = row[column_index]
                break
    return first_row


def get_columns_json(df: pd.DataFrame) -> list:

    columns = list()
    column_names = df.columns.values.tolist()
    df_list = df.values.tolist()
    for column_name_index in range(df.shape[1]):
        first_row = get_frist_row(df_list, column_name_index)
        column_name = column_names[column_name_index]
        d_type = df[column_name].dtype
        if d_type == list:
            if (type(first_row) == list):
                if (len(first_row) and (type(first_row[0]) in [list, object, dict])):
                    prefix = column_name+"."
                    new_df = json_normalize(first_row)
                    somethings = [{**s, "field": prefix+s["field"]}
                                  for s in get_columns_json(new_df)]
                    columns += (somethings)

                    continue
        column_data = {
            SCHEMA_SELECTED_KEY: True,
            SCHEMA_FIELD_KEY: column_name,
            SCHEMA_TYPE_KEY:  get_data_type(first_row),
            SCHEMA_EXAMPLE_DATA_KEY:  first_row,
            SCHEMA_DESCRIPTION_KEY: ""
        }
        columns.append(column_data)
    return columns
