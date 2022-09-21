from .json_utills_v1 import * 
import time

def json_main(data,logger):
    _data = data.get("data")
    _schema = data.get("schema")
    _params = data.get("param",{})
    _path_filter = _params.get("data")


    if _data == None :
        logger.info("Data is None")
        return {
            "data" : [],
            "schema" : []
        }
    if not isinstance(_path_filter,str):
        _path_filter = ""
    start1 = time.time()
    filter_results = json_filter(_data,_path_filter)
    end1 = time.time()

    logger.info("Path filtering done")
    if (_schema is None or len(_schema) == 0):
        logger.info("Schemas is None")
        _data = filter_results[0:1000]

        start2 = time.time()
        _schema = create_schema(_data)
        end2 = time.time()
        start3 = time.time()
        _schema = create_schema_with_parent(_data,_schema)
        end3 =time.time()
    logger.info('Transform data')
    
    if data.get("no_map"):
        final_data = filter_results
    else :
        start4 = time.time()
        final_data = map_data_with_schema(filter_results,_schema)
        end4 = time.time()

    print("Step 1 ",end1-start1)
    print("Step 2 ",end2-start2)
    print("Step 3 ",end3-start3)
    print("Step 4 ",end4-start4)
    return {
        "data" :  final_data,
        "schema" : _schema,
    }

