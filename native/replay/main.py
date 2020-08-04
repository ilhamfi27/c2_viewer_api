import psycopg2, json, asyncio, json, logging, websockets, redis
from datetime import timedelta, datetime
import datetime as dt
from functools import reduce
import numpy as np
from collections import Counter
import hashlib, math
from operator import concat

conn = psycopg2.connect("host=127.0.0.1 \
    dbname=shiptrack_kurdi \
    user=postgres \
    password=1234"
)

r = redis.Redis(host='localhost', port=6379, db=3)

UPDATE_RATE = 1
cur = conn.cursor()
DB_SCHEMA = "public"
cur.execute("SET search_path TO {}". format(DB_SCHEMA))