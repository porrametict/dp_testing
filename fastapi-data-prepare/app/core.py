import time
from io import BytesIO,StringIO

from app.dataprepare.config import *
from app.dataprepare.helpers import *
from app.dataprepare.prepare_csv import csv_prepare_df
from app.dataprepare.prepare_excel import excel_prepare_df
from app.dataprepare.prepare_json import json_main
from app.dataprepare.json_path_filter import JsonManager



def json_filter(data_dict,settings) : 
    _data = data_dict.get("data")
    _path_filter = data_dict.get("json_parse")
    parsed_data = JsonManager.filter(_data,_path_filter)
    schemas = JsonManager.create_schema(parsed_data)
    schemas = JsonManager.create_schema_with_parent(_data,schemas)
    
    result = {
        "parsed_data" : parsed_data,
        "schemas" : schemas
    }

    return result 
    

def preview_data(data_dict,settings,logger) :
    _request_method = data_dict['param'].get("request_method","GET")
    _http_headers = data_dict['param'].get("http_headers",{})
    _json_body = data_dict['param'].get("request_body",{})

    if not data_dict.get("content_type") :
        r = http_request(data_dict['url'],
                        method=_request_method,
                        headers=_http_headers,
                        json_body=_json_body)

        data_dict['content_type'] = get_format(r.headers['content-type'])
    _content_type = data_dict['content_type'].upper()

    
    if _content_type == JSON_FORMAT :
        data_dict['data'] = r.json()
        rj = json_main(data_dict,logger)
        result = {
            "text_data" : parse_alien_language(r),
            "parsed_data" : rj['data'],
            "schemas" : rj['schema'],
        }
    elif _content_type == CSV_FORMAT : 

        df = csv_prepare_df(_file=BytesIO(r.content),
                            _file2=StringIO(r.text),
                            _param=data_dict['param'],
                            logger=logger
                            )
        if 'schemas' not in data_dict or not data_dict['schemas'] :
            data_dict['schemas'] = get_columns(df)
        
        result = {
            "text_data" : parse_alien_language(r),
            "parsed_data" : dataframe_to_list(df),
            "schemas" : data_dict['schemas'],
        }
    elif _content_type in EXCEL_FORMAT_LIST :
        _config_dict = {
            "excel_sheet_num" : data_dict['param']['excel_sheet_num'],
            "h_start_index" : data_dict['param']["h_start_index"],
            "h_end_index" :data_dict['param']['h_end_index']
        }
        
        df = excel_prepare_df(_file=BytesIO(r.content),
                            _param=_config_dict,
                            logger=logger
                        )

        if 'schemas' not in data_dict or not data_dict['schemas'] :
            data_dict['schemas'] = get_columns(df)
        
        result = {
            "text_data" : parse_alien_language(r),
            "parsed_data" : dataframe_to_list(df),
            "schemas" : data_dict['schemas'],
        }
    elif _content_type in IMAGE_FORMAT_LIST :
        result = {
            "text_data" : "text data will not show",
            "parsed_data" : r.url,
            "schemas" : [],
        }
    elif _content_type  == TEXT_FORMAT or _content_type == XML_FORMAT :
        _text = parse_alien_language(r)
        result = {
            "text_data" : _text,
            "parsed_data" : _text,
            "schemas" : [],
        }
    
    result =  {
            "parsed_data" : result['parsed_data'],
            "text_data" : result['text_data'],
            "schemas" : result['schemas'],
            "content_type" : data_dict['content_type'],
            "url" : data_dict["url"],
            "param"  : data_dict['param']
    }

    return result

def prepare_data(data_dict,settings,logger) :
    
    _content_type = data_dict['content_type'].upper()

    logger.info("Content type: "+_content_type)

    if _content_type == JSON_FORMAT :
        rj = json_main(data_dict,logger)
        result =  {
            "parsed_data" : rj['data']
        }
    elif _content_type == CSV_FORMAT : 
        df = csv_prepare_df(
            _file=data_dict.get("url"),
            _param=data_dict.get("param",{}),
            logger=logger,
            is_url=True
        )

        if 'schemas' not in data_dict or not data_dict['schemas'] :
            data_dict['schemas'] = get_columns(df)
        
        result = {
            "parsed_data" : map_data_with_schema(df,data_dict['schemas'])
        }

    elif _content_type  in EXCEL_FORMAT_LIST :
        df = excel_prepare_df(
            _file=data_dict.get("url"),
            _param=data_dict.get("param",{}),
            logger=logger,
            is_url=True
        )

        if 'schemas' not in data_dict or not data_dict['schemas'] :
            data_dict['schemas'] = get_columns(df)

        result = {
            "parsed_data" : map_data_with_schema(df,data_dict['schemas'])
        }
    else :
        result = {
            "parsed_data" : []
        }
    logger.info("Preparation done.")
    return result
         

def local_file_preview(data_dict,settings,logger) :
    
    if data_dict['param']['is_csv'] :
        df = csv_prepare_df(
            _file=data_dict['file'],
            _param=data_dict.get("param",{}),
            logger=logger
        )
    else :
        df = excel_prepare_df(
            _file=data_dict['file'],
            _param=data_dict.get("param",{}),
            logger=logger
        )
    
    if "schemas" not in data_dict or not data_dict['schemas'] :
        data_dict['schemas'] = get_columns(df)
    
    df_list = dataframe_to_list(df)
    del df_list[0]

    result = {
        "parsed_data" : df_list,
        "schemas" : data_dict['schemas'] 
    }
    return result 
    
     
def local_file_prepare(data_dict,settings,logger) :

    ckan_sysadmin_api_key = settings.ckan_sysadmin_api_key
    
    resource_id = data_dict.get("resource_id")
    logger.info("Preparation resource: %s", resource_id)
    _resource = {} 
    try :
        logger.info("Get resource_show: "+resource_id)
        _resource = get_resource_show(resource_id,settings)
    except Exception as e:
        logger.error("Error during get resource_show: {}".format(str(e)))

    try :
        _resource =  _resource['result']

        url = _resource.get("url")
        
        _schema = _resource.get("schemas_json")
        data_dict['schemas']= _schema.get("data")
        l_config = _resource.get("l_config")
        _param = l_config
        _param['http_headers'] = {
                "Authorization" :  ckan_sysadmin_api_key
            }
        _param['request_method'] = "GET"
        
        logger.info("Content type: "+_resource['format'])
        if _resource['format'] == CSV_FORMAT :
            df = csv_prepare_df(
                _file=url,
                _param=_param,
                is_url=True,
                logger=logger
            )
        elif _resource['format'] in EXCEL_FORMAT_LIST :
            df = excel_prepare_df (
                _file=url,
                _param=_param,
                is_url=True,
                logger=logger
            )

        if "schemas" not in data_dict or not data_dict['schemas'] :
            data_dict['schemas'] = get_columns(df)
        

        result = {
            "parsed_data" : map_data_with_schema(df,data_dict['schemas']),
            "schemas" : data_dict['schemas']
            
        }

        #===== 
        body = {
            "success" : True,
            "resource_id" : data_dict.get("resource_id"),
            "job_id" :data_dict['job_id'],
            "schemas"  :result['schemas'],
            "data" : result['parsed_data']

        }

        logger.info("Preparation done.")
        logger.info("Callback to CKAN : {}".format(settings.local_file_callback_url))
        
        # CALLBACK


        c_url = settings.local_file_callback_url
        http_header = {
            "Authorization":  settings.ckan_sysadmin_api_key
        }
        body = body
        time.sleep(8)  # wait for xloader /. xloader wait ckan 5 seconds
        r = requests.post(c_url, headers=http_header, json=body)
    except Exception as e:
        result = {}
        logger.error("Data preparation failure: {}".format(str(e)))

    return result