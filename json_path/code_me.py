import json
from jsonpath_ng import jsonpath, parse

from json_path.json_filter_v1 import json_main


if __name__ == '__main__':
    # ============ load file
    my_json = None 
    f= open('example_source/json_users.json')
    my_json = json.load(f)
    f.close()

    # ==========
    
    data = {
        "data" :my_json,
        "param": {"data": "$.[*].address.geo.some_list[*].item"}
    }
    results = json_main(data,None)

    # ==== write json file 
    json_obj  = json.dumps(results,indent=4)
    with open("j_out.json","w") as outfile:
        outfile.write(json_obj)
    print("done")