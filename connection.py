import psycopg2

HOST = '127.0.0.1'
DB_NAME = 'c2viewer'
USER = 'postgres'
PASSWORD = 'bismillah'

def getconn():
    return psycopg2.connect("host={} \
        dbname={} \
        user={} \
        password={}".format(HOST, DB_NAME, USER, PASSWORD)
    )
