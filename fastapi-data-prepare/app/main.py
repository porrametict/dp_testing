import io
import logging
import uuid
from typing import Optional
from functools import lru_cache

from fastapi import FastAPI, Request, Depends, BackgroundTasks, File, Form

from app.dataprepare import *
from app.dataprepare.config import *
from app.config import Settings
from app.logging_handler import StoringHander
from app.core import (
    json_filter,
    preview_data,
    prepare_data,
    local_file_preview,
    local_file_prepare,

)

app = FastAPI()


@lru_cache()
def get_settings():
    return Settings()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/settings")
def show_settings(settings: Settings = Depends(get_settings)):
    return {
        "ckan_sysadmin_api_key": settings.ckan_sysadmin_api_key,
        "ckan_site_url": settings.ckan_site_url,
    }


@app.post("/json-filter")
async def jf(request: Request, settings: Settings = Depends(get_settings)):
    data = await request.json()
    result = json_filter(data, settings)
    return {
        "success": True,
        "result": {
            "schemas": result['schemas'],
            "parsed_data": result['parsed_data']
        }
    }


@app.post("/preview-data")
async def pv_data(request: Request, settings: Settings = Depends(get_settings)):
    logger = logging.getLogger(__name__)

    data = await request.json()
    try :
        result = preview_data(data, settings, logger)

        return {
            "success": True,
            "result": {
                "parsed_data": result['parsed_data'],
                "text_data": result['text_data'],
                "schemas": result['schemas'],
                "content_type": result['content_type'],
                "url": result["url"],
                "param": result['param']
            }
        }
    except Exception as e :
        return {
            "success" : False,
            "error" : str(e)
        }

@app.post("/prepare-data")
async def pp_data(request: Request, settings: Settings = Depends(get_settings)):
    data = await request.json()

    # set-up logging to the db
    job_id = data['resource_id']
    resource_id = data.get('resource_id')

    headler = StoringHander(resource_id, job_id, settings)
    level = logging.DEBUG
    headler.setLevel(level)
    _logger_id = str(uuid.uuid4())
    logger = logging.getLogger(_logger_id)
    headler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(headler)
    # also show logs on stdrr
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)
    logger.info("Preparation resource: %s", resource_id)

    # logger.info("Content type: %s",_content_type)
    try :
        result = prepare_data(data, settings, logger)
        result['error'] = None
        is_success = True
    except Exception as e :
        result = {}
        result['error'] = str(e)
        result['parsed_data'] = []
        is_success = False
        logger.error("Data preparation failure: {}".format(str(e)))
    # delete logger for keep memory
    del logging.Logger.manager.loggerDict[_logger_id]

    return {
        "success": is_success,
        "result": result['parsed_data'],
        "error" : result['error']
    }


@app.post("/local-file-preview")
async def lf_pv(file: bytes = File(...),
                is_csv: bool = Form(...),
                csv_delimeter: Optional[list] = Form([]),
                csv_delimeter_other: Optional[str] = Form(""),
                csv_text_qualifier: Optional[str] = Form(""),
                csv_first_row_is_header: Optional[bool] = Form(True),
                excel_sheet_num: Optional[str] = Form("1"),
                h_start_index: Optional[str] = Form("1"),
                h_end_index: Optional[str] = Form("1"),
                settings: Settings = Depends(get_settings)

                ):
    config_dict = {
        "is_csv": is_csv,
        "csv_delimeter": csv_delimeter,  # list
        "csv_delimeter_other": csv_delimeter_other,
        "csv_text_qualifier": csv_text_qualifier,
        "csv_first_row_is_header": csv_first_row_is_header,
        "excel_sheet_num": int(excel_sheet_num),
        "h_start_index": int(h_start_index),
        "h_end_index": int(h_end_index),

    }
    try :
        _data_dict = {
            "file": io.BytesIO(file),
            "param": config_dict
        }
        logger = logging.getLogger(__name__)
        result = local_file_preview(_data_dict, settings, logger)

        return {
            "success": True,
            "result": {
                "schema": result['schemas'],
                "data_list": result['parsed_data']
            }
        }
    except Exception as e:
        return {
            "success" : False,
            "error" : str(e)
        }


@app.post("/local-file-prepare")
async def lf_pp(request: Request, background_tasks: BackgroundTasks, settings: Settings = Depends(get_settings)):
    data = await request.json()
    # set-up logging
    job_id = data['job_id']
    resource_id = data.get('resource_id')

    handler = StoringHander(resource_id, job_id, settings)
    level = logging.DEBUG
    handler.setLevel(level)
    logger = logging.getLogger(job_id)
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(handler)
    # also show logs on stdrr
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)
    
    background_tasks.add_task(local_file_prepare, data, settings, logger)
    return {"status": "OK"}
