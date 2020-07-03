import psycopg2, json, asyncio, json, logging, websockets, redis
from datetime import timedelta, datetime
import datetime as dt
from functools import reduce
import numpy as np
from collections import Counter
import hashlib, math
from operator import concat

conn = psycopg2.connect("host=127.0.0.1 \
    dbname=c2_viewer_v1 \
    user=postgres \
    password=LenElhan!@#"
)

r = redis.Redis(host='localhost', port=6379, db=3)

UPDATE_RATE = 1
cur = conn.cursor()

