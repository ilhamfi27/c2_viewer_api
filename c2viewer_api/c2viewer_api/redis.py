import redis
import json

from django.conf import settings

REDIS_HOST = settings.REDIS_HOST
REDIS_PORT = settings.REDIS_PORT

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=2)

class RedisUtil:
    def __init__(self, hash):
        self.redis_hash = hash

    def redis_decode_to_dict(self, nested_dict=False):
        # convert string boolean to boolean
        if self.redis_hash != None:
            if nested_dict:
                return { key.decode(): json.loads(val.decode()) for key, val in self.redis_hash.items() }
            return { key.decode(): val.decode() for key, val in self.redis_hash.items() }
        return {}

    def redis_decode_to_list(self):
        # convert string boolean to boolean
        return [json.loads(val) for key, val in self.redis_hash.items()]