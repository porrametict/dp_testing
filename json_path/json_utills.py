import json
import re
from jsonpath_ng import parse
import pandas as pd
from .my_json import get_columns_json 
import numpy as np
from .config import * 
import time
import flatten_json
def df_nan_to_none(df):
    df = df.where(pd.notnull(df), None)
    df = df.replace(np.NaN, None)
    df = df.replace({np.nan: None})

    return df

def json_filter(_data,path_filter):
    path_filter = path_filter.strip()
    if not path_filter :
        if (isinstance(_data,dict)) :
           return [_data]
        else:
            return _data
    

    else :
        jsonpath_expr = parse(path_filter)
        return [match.value for match in jsonpath_expr.find(_data)]


def create_schema(json_data):
    df = pd.json_normalize(json_data)
    schema = get_columns_json(df)
    return schema


def create_schema_with_parent(json_data,schema):
    max_json_level = get_json_depth(json_data)

    def get_first_child_index(schema,key_word):
        for i in range (len(schema)):
            if f"{key_word}" in schema[i][SCHEMA_FIELD_KEY] : 
                return i
    


    for i in range(0,max_json_level):
        tmp_df = pd.json_normalize(json_data,max_level=i)
        tmp_schema = get_columns_json(tmp_df)

        for j in tmp_schema :
            if j not in schema :
                insert_index = get_first_child_index(
                    schema,j[SCHEMA_FIELD_KEY]
                )
                if insert_index :
                    j['is_parent'] = True
                    j[SCHEMA_SELECTED_KEY] =False
                    schema.insert(insert_index,j)
    return schema



def map_data_with_schema(data,_schema):
       
    schema_selected_items = list(filter(lambda item: SCHEMA_SELECTED_KEY in item and  item[SCHEMA_SELECTED_KEY] ,_schema))
    schema_selected_keys = [i[SCHEMA_FIELD_KEY] for i in schema_selected_items] 
    schema_parent_items = list(filter(lambda item: 'is_parent' in item and item['is_parent'],_schema))
    schema_parent_keys = [i[SCHEMA_FIELD_KEY] for i in schema_parent_items] 
    
    def gen_fields():
        return { i:None for i in  schema_selected_keys}


    results = [ gen_fields() for i in range(len(data))]




    
    for j in range(len(data)):
        tmp_dict = flatten_json_keep_parent(data[j],schema_parent_keys)
    
        for key,value in tmp_dict.items():
            if key in schema_selected_keys and results[j][key] is None :
                results[j][key] = value 


    def get_new_field_name(item):
        return item[SCHEMA_NEW_FIELD_NAME] if SCHEMA_NEW_FIELD_NAME in item.keys(
            ) and item[SCHEMA_NEW_FIELD_NAME].strip() else ""

    fields_map = {}
    for i in range(len(_schema)) :
        _field = _schema[i][SCHEMA_FIELD_KEY]
        
        if (_field in schema_selected_keys):
            _new_field = get_new_field_name(_schema[i])
            
            if _new_field and _new_field != _field :
                fields_map[_field] = _new_field



    for i in range(len(results)):
            for _field in fields_map.keys():
                itemObj = results[i]
                new_field_name = fields_map[_field]
                if(_field in itemObj):
                    itemObj[new_field_name] = itemObj[_field]
                    del itemObj[_field]






    return results

def get_flatten_parent_key(key,parent_key):
    a_list = key.split(".")
    try :
        p_index = a_list.index(parent_key)
        return ".".join(a_list[p_index:])
    except :
        return key
def flatten_json_keep_parent(data,parent_keys=[]):
    
    f_data = flatten_json.flatten_json(data,".")






    parent_data = {get_flatten_parent_key(key,p_key):value for key,value in f_data.items()  for p_key in parent_keys if f"{p_key}." in key}

    f2_data  = flatten_json.unflatten_list(parent_data,".")
    f2_data.update(f_data)

    return f2_data


def get_json_depth (json_data):
    n_data = json_data[0:100]
    df= pd.json_normalize(n_data)
    cols = df.columns.tolist()
    return max([i.count(".")+1 for i in cols])


