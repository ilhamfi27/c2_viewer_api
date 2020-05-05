from datetime import datetime
import json

def datetime_to_string(table_results):
    # convert datetime to string
    for key, value in table_results.items():
        if type(value) == datetime:
            table_results[key] = value.strftime('%Y-%m-%dT%H:%M:%S:%f')

def string_bool_to_bool(table_results):
    # convert string boolean to boolean
    for key, value in table_results.items():
        if type(value) == str and (value.upper() == "TRUE" or value.upper() == "FALSE"):
            table_results[key] = True if value.upper() == 'TRUE' else False

def redis_decode_to_dict(redis_hash):
    # convert string boolean to boolean
    return { key.decode(): json.loads(val) for key, val in redis_hash.items() }

def redis_decode_to_list(redis_hash):
    # convert string boolean to boolean
    return [ json.loads(val) for key, val in redis_hash.items() ]

def redis_decode_to_dict(redis_hash):
    # convert string boolean to boolean
    return { key: val for key, val in redis_hash.items() }
