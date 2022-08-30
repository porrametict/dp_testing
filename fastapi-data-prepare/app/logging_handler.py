import datetime
import logging
import requests
import six 
import json

class StoringHander(logging.Handler) :

    def __init__(self,resource_id,task_id,settings) :
        logging.Handler.__init__(self)
        self.task_id = task_id
        self.resource_id = resource_id
        self.store_uri = f"{settings.dataconnect_store_log_url}"
        self.task_show_url = f"{settings.task_status_show_url}"
        self.task_update_url = f"{settings.task_status_update_url}"
        self.http_header = {
                    "Authorization" :  settings.ckan_sysadmin_api_key
            }


    def emit (self,record) :
        _message = six.text_type(record.getMessage())

        _data = {
        "resource_id"  : self.resource_id,
        "task_id" : self.task_id,
        "timestamp": str(datetime.datetime.utcnow()),
        "message" : _message,
        "level" : six.text_type(record.levelname),
        "module" : six.text_type(record.module),
        "funcName" : six.text_type(record.funcName),
        "lineno":record.lineno,
        }
        try :
            r = requests.post(self.store_uri,headers=self.http_header,
            json=_data,timeout=5)
        except Exception as e: 
            pass 


    