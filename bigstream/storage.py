from bigstream import bsdata
import requests
import json


class StorageService:
    def __init__(self, url='http://localhost:19080/v1.2', token=None):
        self.api_url = url
        self.token = token

    def list(self):
        s = '/storage'
        resp = self._bs_request(s_url=s)
        return resp.json()

    def get_storages(self):
        storage_list = self.list()
        ret = {}
        for sname in storage_list:
            ss = Storage(service=self,sname=sname)
            ret[sname] = ss
        return ret

    def storage(self,name):
        return Storage(service=self,sname=name)

    def stats(self,storage_name):
        s = '/storage/' + storage_name + '/stats'
        resp = self._bs_request(s_url=s)
        ret=None
        if resp.status_code==200:
            ret = resp.json()
        
        return ret

    def delete(self,storage_name):
        s = '/storage/' + storage_name 
        resp = self._bs_request(s_url=s,method='DELETE')
        ret=False
        if resp.status_code==200:
            ret = True
        
        return ret

    def write(self,storage_name,payload=[]):
        s = '/storage/' + storage_name 
        resp = self._bs_request(s_url=s,pl=payload,method='PUT')
        ret=False
        if resp.status_code==200:
            ret = True
        
        return ret

    def read(self,storage_name,params={}):
        s = '/storage/' + storage_name + '/objects'
        resp = self._bs_request(s_url=s,prm=params)
        ret=[]
        if resp.status_code==200:
            ret = resp.json()
        
        return ret

    def read_object(self,address,params={}):
        s = '/object/' + address
        resp = self._bs_request(s_url=s,prm=params)
        ret=None
        if resp.status_code==200:
            ret = resp.json()
        
        return ret

    def _bs_request(self,s_url='/',prm={},method='GET',pl={}):
        url = self.api_url + s_url
        headers = {}

        if self.token is not None:
            headers['Authorization'] = 'Bearer ' + self.token
        
        if method=='PUT' or method=='POST':
            headers['Content-Type'] = 'application/json'

        resp = requests.request(method, url,params=prm, headers=headers, data = json.dumps(pl))

        return resp


class Storage:
    def __init__(self, service, sname):
        self.service = service
        self.name = sname
    
    def __repr__(self):
        return self.name

    def __str__(self):
        return self.__repr__()

    def exist(self):
        return self.stats() is not None
    
    def stats(self):
        return self.service.stats(
            storage_name=self.name
            )

    def put(self,obj):
        o = None
        if type(obj) is dict:
            o = Object(obj)
        elif type(obj) is Object:
            o = obj
        else:
            return False

        return self.service.write(
            storage_name=self.name,payload=o.as_payload()
        )
    
    def object(self,id=None,seq=None,key=None):        
        return self.get_object(id,seq,key)

    def get_object(self,id=None,seq=None,key=None):
        obj=self.read_object(id,seq,key)
        ret=None
        if obj is not None:
            ret = Object(obj)
        
        return ret

    def read_object(self,id=None,seq=None,key=None):
        adds = self.name
        if id is not None:
            adds = self.name + '$' + str(id)
        elif seq is not None:
            adds = self.name + '$[' + str(seq) + ']'
        elif key is not None:
            adds = self.name + '${' + str(key) + '}'

        ret=self.service.read_object(adds)

        return ret
    
    def read(self,params={}):
        return self.service.read(
            storage_name=self.name,
            params=params
            )
    
    def read_all(self,field='',data_only=False):
        res = []
        params={'from':1}

        stats = self.stats()
        if stats==None:
            return []

        count = stats['count']

        if data_only:
            field='_data'

        params['field'] = field

        while len(res)<count:
            objs =  self.read(params)
            if len(objs)>0:
                res+=objs
                params['from'] = len(res) + 1
            else:
                break
        return res

class Object:
    def __init__(self,obj={},id=None,meta=None,data=bsdata.BSData()):
        self.id = id
        self.meta = meta
        self.data = bsdata.create(data)
        
        if '_id' in obj.keys():
            self.id = obj['_id']
        
        if 'meta' in obj.keys():
            self.meta = obj['meta']
        
        if 'data' in obj.keys():
            self.data = bsdata.create(obj['data'])
    
    def __repr__(self):
        return json.dumps(self.as_dict())

    def __str__(self):
        return self.__repr__()

    def as_dict(self):
        ret = {'meta':self.meta, 'data':self.data.as_dict() }
        if self.id is not None:
            ret['_id'] = self.id

        return ret
    
    def as_payload(self,obj_decode=True):
        ret = self.as_dict()
        ret['data'] = self.data.as_payload(obj_decode=obj_decode)
        if type(ret['meta']) is not dict:
            del ret['meta']

        return ret