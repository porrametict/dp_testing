from jsonpath_ng import parse
import pandas as pd
import numpy as np
from .config import * 
from .my_json import get_columns_json
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
    df = df_nan_to_none(df)
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
    max_json_level = get_json_depth(data)


    schema_selected_keys = list(filter(lambda item: SCHEMA_SELECTED_KEY in item and  item[SCHEMA_SELECTED_KEY] ,_schema))

    def gen_fields():
        return { i[SCHEMA_FIELD_KEY]:None for i in  schema_selected_keys}
    
    # set empty value for each key.
    results = [ gen_fields() for i in range(len(data))]


    
    # assign value
    for i in range(0,max_json_level):
        tmp_df = pd.json_normalize(data,max_level=i)
        tmp_dict = tmp_df.to_dict("records")

        for j in range(len(tmp_dict)):
            j_dict = tmp_dict[i]
            for key,value in j_dict.items():
                if key in schema_selected_keys and results[j][key] is None :
                    results[j][key] = value  
    
    # rename key to new new_field_name
    # get new_field_name

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
    # rename 
    for i in range(len(results)):
            for _field in fields_map.keys():
                itemObj = results[i]
                new_field_name = fields_map[_field]
                if(_field in itemObj):
                    itemObj[new_field_name] = itemObj[_field]
                    del itemObj[_field]
    
    df = pd.json_normalize(results,max_level=0)
    df = df_nan_to_none(df)
    results = df.to_dict("records")
    return data




def get_json_depth (json_data):
    n_data = json_data[0:100]
    df= pd.json_normalize(n_data)
    cols = df.columns.tolist()
    return max([i.count(".")+1 for i in cols])