from base.config import *
import psycopg2

def getconn():
    return psycopg2.connect("host={} \
        dbname={} \
        user={} \
        password={}".format(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)
    )
