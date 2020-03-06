import psycopg2, json, asyncio, json, logging, websockets
from datetime import timedelta, datetime
import datetime as dt
from functools import reduce
import numpy as np
from collections import Counter
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.config import getconn

conn = getconn()

UPDATE_RATE = 600
cur = conn.cursor()

