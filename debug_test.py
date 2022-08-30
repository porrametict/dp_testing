import logging
import json
from code.json.prepare_json import json_main
import math
import json

logger = logging.getLogger(__name__)

def read_file(fname):
    data = None
    f = open(fname,"r")
    data = json.loads(f.read())
    f.close()
    return data



if __name__ == "__main__":
    data = read_file("output/error_json.json")
    s_data = {"data":data,
    "param" : {"data":"result"}
    }

    def eiei ():
        json_main(s_data,logger)

    result = json_main(s_data,logger)
    print(result['data'])
    

    print("=================================")

