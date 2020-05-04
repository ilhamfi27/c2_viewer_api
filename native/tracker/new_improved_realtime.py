import asyncio
import json
import logging
import websockets
import random
import psycopg2
import numpy as np
from functools import reduce
from datetime import datetime
from tracker.config import getconn

conn = getconn()
cur = conn.cursor()

now = datetime.now()

current_time = now.strftime("%Y-%m-%d %H:%M:%S")

def main():
    last_created_time = [
        '0000-00-00 00:00:00',
        '0000-00-00 00:00:00',
        '0000-00-00 00:00:00',
        '0000-00-00 00:00:00',
        '0000-00-00 00:00:00',
        '0000-00-00 00:00:00',
        '0000-00-00 00:00:00',
        '0000-00-00 00:00:00'
    ]

    try:
        q = """
            SELECT st.* 
            FROM replay_system_track_general st 
            JOIN sessions s ON st.session_id=s.id 
            JOIN (
                    SELECT
                        session_id,
                        system_track_number,
                        max(created_time) created_time 
                    FROM replay_system_track_general 
                    WHERE created_time>= $last_check_time 
                    AND created_time< $current_time 
                    GROUP BY session_id,system_track_number
                ) mx 
            ON st.system_track_number=mx.system_track_number 
            and st.created_time=mx.created_time 
            and st.session_id=mx.session_id 
            WHERE s.end_time is NULL 
            and st.own_unit_indicator='FALSE' 
            ORDER BY st.system_track_number;
            """.format(current_time)
        cur.execute(q)
        data = cur.fetchall()
        print(data)
        # for idx, row in enumerate(data):
        #     session_id = row[2]
    except psycopg2.Error as e:
        print(e)
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
