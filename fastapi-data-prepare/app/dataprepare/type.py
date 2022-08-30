import re
from dateutil import parser


def get_data_type(data):
    data = str(data)
    if re.search("[\.\,eKFLi]",data): # ค่าคงตัวคณิตศาสตร์
        return "text"

    try:
        float(data)
        return "numeric"
    except:
        pass

    try:
        parser.parse(data)
        return "timestamp"
    except:
        pass

    return "text"
