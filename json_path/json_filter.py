from .json_utills import * 


def json_main(data,logger):
    _data = data.get("data")
    _schema = data.get("schema")
    _params = data.get("param",{})
    _path_filter = _params.get("data")


    if _data == None :
        # logger.info("Data is None")
        return {
            "data" : [],
            "schema" : []
        }
    if not isinstance(_path_filter,str):
        _path_filter = ""

    filter_results = json_filter(_data,_path_filter)

    # logger.info("Path filtering done")
    if (_schema is None or len(_schema) == 0):
        # logger.info("Schemas is None")
        _data = filter_results[0:1000]

        _schema = create_schema(_data)


        _schema = create_schema_with_parent(_data,_schema)

    
    # logger.info('Transform data')
    
    if data.get("no_map"):
        final_data = filter_results
    else :

        final_data = map_data_with_schema(filter_results,_schema)

    return {
        "data" :  final_data,
        "schema" : _schema,
    }

