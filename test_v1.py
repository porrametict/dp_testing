import json
from jsonpath_ng import jsonpath, parse
import write_csv

import logging
import json
# from code_me.json.prepare_json import json_main
import write_csv
import cProfile, pstats, io
import psutil
import math
import json
from pstats import SortKey
import re
import os
from memory_profiler import profile

from json_path.json_filter_v1 import json_main
logger = logging.getLogger(__name__)

def read_file(fname):
    data = None
    f = open(fname,"r")
    data = json.loads(f.read())
    f.close()
    return data

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])


# @profile
def test_main (n_records) :
    f_name = f"output/normal_{n_records}.json"
    f_stats = os.stat(f_name)
    data = read_file(f_name)

    PPATH = "$.[*].field1.address"
    PPATH = ""
    def eiei ():
        json_main({"data":data,"param":{"data":PPATH}},logger)

    pr = cProfile.Profile()
    pr.enable()
    # eiei()
    process = psutil.Process(eiei())
    mem  = process.memory_info().rss
    pr.disable()
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
    ps.print_stats()
    pf = s.getvalue()

    r_result = re.search(r"in (.*) seconds", pf)

    used_time = r_result.group(1) # process time in seconds



    # with open(f'json_test_results/{n_records}.txt', 'w+') as f:
    #     f.write(pf)
    
    used_mem = mem
    # used_mem =0

    return [n_records,convert_size(f_stats.st_size),used_time,convert_size(used_mem)]




if __name__ == "__main__":
   results =  test_main(1000000) 
   print(results)
    # for i in range(10000,1000000+10000,10000):
    #     print("Testing : " + str(i))
    #     result = test_main(i)
    #     write_csv.write_result(result)
    #     print(result)