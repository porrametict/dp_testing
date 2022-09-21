import os 
import math
import uuid 
import json



def random_string():
    return  str(uuid.uuid4())

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])

# bytes
def get_file_size(filename):
    return os.path.getsize(filename)



def gen_json_with_attrs(num):
    results = dict()
    for i in range(num):
        _rs = random_string()
        results[f"key_{_rs}"] =f"value_{_rs}"
    return results
    
    


def write_file(data,filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)



def one_layer_dict_n_attrs (n):
    data = gen_json_with_attrs(n)
    write_file(data,f'output/one_layer_dict_{n}_attrs.json')


def gen_n_layer_dict(n):
    results = dict()
    _rs = random_string()

    if n == 1 :
        results[f"key_{_rs}"] =f"value_{_rs}"
        return results
    else :
        results[f"key_{_rs}"] = gen_n_layer_dict(n-1)
        return results



def n_layers_dict(n):
    data = gen_n_layer_dict(n)
    write_file(data,f'output/{n}_layers_dict.json')
   

def gen_n_layer_n_dict(max_n,n):
    results = []
    _field = f"field_{n}"
    if n ==1 :
        n_dict  = {}
        _rs = random_string()
        for i in range(3) :
            n_dict.update({f"{_field}_{i}": _rs})  
        return n_dict
    else :
        n_dict = {}
        for i in range(n):
            data = gen_n_layer_n_dict(max_n,n-1 )
            n_dict.update({f"{_field}_{i}":data})
        if n == max_n :
            results.extend(n_dict.values())
            return results
        else :
            return n_dict
def  n_nested_layers_n_dict(n):
    data = gen_n_layer_n_dict(n,n)
    write_file(data,f'output/{n}_nested_json.json')



def gen_json_from_template(n):
    data = None
    f = open("template/json_template_1.json",'r')
    data  = json.load(f)
    f.close()
    
    return [data for i in range(n)]
def normal_json(n):
    data  = gen_json_from_template(n)
    write_file(data,f'output/normal_{n}.json')
# =========================================================================


if __name__ == '__main__':
    # one_layer_dict_n_attrs(100)
    # n_layers_dict(450)
    # n_nested_la
    # yers_n_dict(8)
    for i in range(10000,1000000+10000,10000):
        print("start ",i)
        normal_json(i)