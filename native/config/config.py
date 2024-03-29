import os
from dotenv import load_dotenv
from os.path import dirname
import psycopg2
import redis

env_path = dirname(dirname(__file__)) + '/.env'
load_dotenv(dotenv_path=env_path)

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_SCHEMA = os.getenv('DB_SCHEMA') if os.getenv('DB_SCHEMA') not in [None, ""] else "public"
WS_HOST = os.getenv('WS_HOST')
WS_PORT = os.getenv('WS_PORT')

def getconn():
    return psycopg2.connect("host={} \
        dbname={} \
        user={} \
        password={}".format(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)
    )

REDIS_HOST=os.getenv('REDIS_HOST')
REDIS_PORT=os.getenv('REDIS_PORT')
REDIS_DB=os.getenv('REDIS_DB')

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
