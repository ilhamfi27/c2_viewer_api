import psycopg2

conn = psycopg2.connect("host=127.0.0.1 \
    dbname=c2viewer \
    user=postgres \
    password=bismillah"
)

UPDATE_RATE = 5
cur = conn.cursor()
