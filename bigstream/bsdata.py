import json
import base64

class BSData:
    def __init__(self,data_type='object',data={},encoding=None):
        self.type = data_type
        self.data = data
        self.encoding = encoding

    def __repr__(self):
        return json.dumps(self.as_dict())

    def __str__(self):
        return self.__repr__()

    def value(self,decode='binary'):
        return self.decode(decode)

    def decode(self,typ='binary'):
        ret = self.data
        if type(self.data) is str and self.encoding == 'base64':
            ret = base64.b64decode(self.data)
            if typ in ['utf-8','text']:
                ret = ret.decode('utf-8')

        return ret

    def as_dict(self):
        ret = {
            'object_type':'bsdata',
            'data_type':self.type,
            'data':self.data
        }
        if self.encoding is not None:
            ret['encoding'] = self.encoding

        return ret
    
    def as_payload(self,obj_decode=True):
        ret = self.as_dict()
        if obj_decode and self.type == 'object':
            ret = self.data

        return ret

def binary_file(filename):
    encoded_string = ""
    with open(filename, "rb") as data_file:
        encoded_string = base64.b64encode(data_file.read()).decode("utf-8") 

    return BSData('binary',encoded_string,'base64')
    
def binary_base64(base64_text):
    return BSData('binary',base64_text,'base64')

def binary_data(data):
    encoded_string = base64.b64encode(data).decode("utf-8") 
    return BSData('binary',encoded_string,'base64')

def create(obj):
    ret = None
    if isinstance(obj,BSData):
        ret=obj
    elif type(obj) is str:
        ret=BSData('text',obj)
    elif type(obj) is dict:
        if obj.get('object_type') == 'bsdata' and obj.get('data_type') in ['text','object','binary']:
            ret=BSData(obj['data_type'],obj['data'],obj.get('encoding'))
        else:
            ret=BSData('object',obj)
        
    return ret


    