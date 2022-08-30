from typing import Any
import pandas as pd
import math

from .json import get_columns_json
from .config import *
from .helpers import *


class JsonNode:
    def __init__(self, path: str, value: Any, value_type: str, level: int, parent_node: object):
        self.path = path
        self.level = level
        self.value = value
        self.value_type = value_type
        self.parent_node = parent_node or None
        self.children = []

    def __str__(self):
        return f'JsonNode(path="{self.path}",level={self.level},value_type={self.value_type},value={self.value})'

    def __repr__(self):
        return f'JsonNode(path="{self.path}",level={self.level},value_type={self.value_type},value={self.value})'

    def get_all_children(self):
        result_list = []
        if len(self.children) == 0:
            result_list.append(self)
            return result_list
        else:
            for child in self.children:
                result_list.extend(child.get_all_children())
            return result_list

    def get_all_children_node_paths(self):
        return [i.path for i in self.get_all_children()]


class JsonManager:

    @staticmethod
    def guess_type(value):
        if type(value) is dict:
            return "dict"
        elif type(value) is list:
            return "list"
        elif type(value) is int or type(value) is float:
            return "number"
        else:
            return "text"

    @staticmethod
    def create_node(json_data, parent_node=None, path=""):

        if parent_node is None:
            current_level = 0
        else:
            current_level = parent_node.level + 1

        value_type = JsonManager.guess_type(json_data)

        if value_type == "dict" and len(json_data) > 0:
            p_node = JsonNode(path=path, level=current_level,
                              value_type=value_type, parent_node=parent_node, value=json_data)
            if parent_node is not None:
                parent_node.children.append(p_node)

            for key, value in json_data.items():
                JsonManager.create_node(
                    json_data=value, parent_node=p_node, path=key)

            return p_node
        elif value_type == "list" and len(json_data) > 0:
            p_node = JsonNode(path=path, level=current_level,
                              value_type=value_type, parent_node=parent_node, value=json_data)
            if parent_node is not None:
                parent_node.children.append(p_node)

            for item in json_data:
                JsonManager.create_node(
                    json_data=item, parent_node=p_node, path="")

            return p_node
        else:
            e_node = JsonNode(
                path=path,
                level=current_level,
                value=json_data,
                value_type=value_type,
                parent_node=parent_node,
            )
            if parent_node is not None:
                parent_node.children.append(e_node)

            return e_node

    @staticmethod
    def create(json_data):
        return JsonManager.create_node(json_data, None)

    @staticmethod
    def condition_transform(condition_text: str, all_path_names: list):

        return condition_text.split('.')

    @staticmethod
    def select_node(node, condition_list: list):

        results = []
        if len(condition_list) == 1:
            condition = condition_list[0]
            for i in node.children:
                if condition == i.path or condition == "":
                    results.append(i)
            return results
        else:
            some_list = []
            condition = condition_list[0]
            del condition_list[0]
            for i in node.children:
                if condition == i.path or condition == "":
                    some_list.extend(
                        JsonManager.select_node(i, [*condition_list]))
            return some_list

    @staticmethod
    def parse_to_value(node_list: list):
        result_dict = dict()
        for i in node_list:
            try :
                i_value = None if math.isnan(i.value) else i.value  
            except :
                i_value = i.value

            if i.parent_node not in result_dict:
                if i.parent_node.value_type == "dict":
                    result_dict[i.parent_node] = dict()
                    result_dict[i.parent_node][i.path] = i_value

                else:
                    result_dict[i.parent_node] = list()
                    result_dict[i.parent_node].append(i_value)
            else:
                if i.parent_node.value_type == "dict":
                    result_dict[i.parent_node][i.path] = i_value
                else:
                    result_dict[i.parent_node].append(i_value)
        results = []
        for key, value in result_dict.items():
            try :
                value = None if math.isnan(value) else value
            except :
                pass 
            if type(value) is list:
                for v_value in value:
                    results.append(v_value)
            elif type(value) is dict:
                results.append(value)
            else:
                pass
        return results

    @staticmethod
    def filter(json_data, path_filter: str):
        json_tree = JsonManager.create(json_data)
        all_path_names = json_tree.get_all_children_node_paths()
        result = JsonManager.condition_transform(path_filter, all_path_names)
        results = JsonManager.select_node(json_tree, result)
        value_results = JsonManager.parse_to_value(results)
        return value_results

    @staticmethod
    def create_schema(json_data):

        df = pd.json_normalize(json_data)
        df = df_nan_to_none(df)
        schema = get_columns_json(df)
        return schema

    @staticmethod
    def create_schema_with_parent(json_data, schema):
        json_tree = JsonManager.create(json_data)

        # create schema
        def get_first_child_index(schema, key_word):
            for i in range(len(schema)):
                if f"{key_word}." in schema[i][SCHEMA_FIELD_KEY]:
                    return i

        json_tree_max_level = max(
            [i.level for i in json_tree.get_all_children()])

        for i in range(0, json_tree_max_level):
            tem_df = pd.json_normalize(json_data, max_level=i)
            tem_schema = get_columns_json(tem_df)
            for j in tem_schema:
                if j not in schema:
                    insert_index = get_first_child_index(
                        schema, j[SCHEMA_FIELD_KEY])
                    if insert_index:
                        j['is_parent'] = True
                        j[SCHEMA_SELECTED_KEY] = False
                        schema.insert(insert_index, j)

        return schema

    @staticmethod
    def map_data_with_schema(data, schema):
        json_tree = JsonManager.create(data)

        schema_selected_keys = []
        for i in schema:
            if SCHEMA_SELECTED_KEY in i and i[SCHEMA_SELECTED_KEY]:
                schema_selected_keys.append(i[SCHEMA_FIELD_KEY])

        results = [dict() for i in range(len(data))]
        # set empty data
        for item in results:
            for i in schema_selected_keys:
                item[i] = None
        # end
        json_tree_max_level = max(
            [i.level for i in json_tree.get_all_children()])

        for i in range(0, json_tree_max_level):
            tem_df = pd.json_normalize(data, max_level=i)
            tmp_dict = tem_df.to_dict("records")

            for j in range(len(tmp_dict)):
                j_dict = tmp_dict[j]
                for key, value in j_dict.items():
                    if key in schema_selected_keys and results[j][key] is None:
                        results[j][key] = value
        # rename key to new new_field_name
        fields_map = {}
        for i in range(len(schema)):
            _field = schema[i][SCHEMA_FIELD_KEY]
            _new_field = schema[i][SCHEMA_NEW_FIELD_NAME] if SCHEMA_NEW_FIELD_NAME in schema[i].keys(
            ) and schema[i][SCHEMA_NEW_FIELD_NAME].strip() else ""
            if (_field in schema_selected_keys) and _new_field and _new_field != _field:
                fields_map[_field] = _new_field
        for i in range(len(results)):
            for _field in fields_map.keys():
                itemObj = results[i]
                new_field_name = fields_map[_field]
                if(_field in itemObj):
                    itemObj[new_field_name] = itemObj[_field]
                    del itemObj[_field]
        # just replace Nan to None
        df = pd.json_normalize(results, max_level=0)
        df = df_nan_to_none(df)
        results = df.to_dict("records")

        return results
