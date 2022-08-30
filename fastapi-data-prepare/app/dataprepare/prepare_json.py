from  .json_path_filter import JsonManager
from .config import * 

def json_main (data,logger) :

    _data = data.get("data")
    _schema = data.get("schemas")
    _param = data.get("param",{})
    _path_filter = _param.get("data")

    
    if _data == None :
        logger.info("Data is None")
        return {
            "data" : [],
            "schema" : []
        }
    if not isinstance(_path_filter,str) :
        _path_filter = ""

    filter_result = JsonManager.filter(_data,_path_filter)

    logger.info("Path filtering done")
    if (_schema is None or len(_schema) == 0) :
        logger.info("Schemas is None")
        _schema = JsonManager.create_schema(filter_result)
        _schema = JsonManager.create_schema_with_parent(filter_result,_schema)

    logger.info("Transform data")

    if data.get("no_map") :
        final_data = filter_result
    else :
        final_data = JsonManager.map_data_with_schema(filter_result,_schema)
        
    return {
        "data" : final_data,
        "schema" : _schema,
    }

