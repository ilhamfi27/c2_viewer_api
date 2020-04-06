import os
from dotenv import load_dotenv
from os.path import dirname
import psycopg2

env_path = dirname(dirname(__file__)) + '/.env'
load_dotenv(dotenv_path=env_path)

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
WS_HOST = os.getenv('WS_HOST')
WS_PORT = os.getenv('WS_PORT')

def getconn():
    return psycopg2.connect("host={} \
        dbname={} \
        user={} \
        password={}".format(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)
    )
