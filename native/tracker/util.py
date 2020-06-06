from datetime import datetime
import json

def datetime_to_string(table_results):
    # convert datetime to string
    for key, value in table_results.items():
        if type(value) == datetime:
            table_results[key] = value.strftime('%Y-%m-%d %H:%M:%S.%f')

def single_string_to_datetime(str):
    return datetime.strptime(str, '%Y-%m-%d %H:%M:%S.%f')

def single_datetime_to_string(time):
    return time.strftime('%Y-%m-%d %H:%M:%S.%f')

def string_bool_to_bool(table_results):
    # convert string boolean to boolean
    for key, value in table_results.items():
        if type(value) == str and (value.upper() == "TRUE" or value.upper() == "FALSE"):
            table_results[key] = True if value.upper() == 'TRUE' else False

def redis_decode_to_list(redis_hash):
    # convert string boolean to boolean
    return [ json.loads(val) for key, val in redis_hash.items() ]

def redis_decode_to_dict(redis_hash, nested_dict=False):
    # convert string boolean to boolean
    if nested_dict:
        return { key.decode(): json.loads(val.decode()) for key, val in redis_hash.items() }
    return { key.decode(): val.decode() for key, val in redis_hash.items() }
