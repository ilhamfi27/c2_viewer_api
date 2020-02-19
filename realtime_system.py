#!/usr/bin/env python

# WS server example that synchronizes REALTIME_state across clients

import asyncio
import json
import logging
import websockets
import random
import psycopg2
import numpy as np
import time
import datetime
from functools import reduce
from stored_data import *

logging.basicConfig()

conn = psycopg2.connect("host=127.0.0.1 \
    dbname=c2viewer \
    user=postgres \
    password=bismillah"
)

cur = conn.cursor()

ar_mandatory_table = [
    'replay_system_track_general',
    'replay_system_track_kinetic',
    'replay_system_track_processing',
    # 'replay_track_general_setting'
]
ar_mandatory_table_8 = [
    'replay_system_track_general',
    'replay_system_track_kinetic',
    'replay_system_track_processing',
    'replay_system_track_identification',
    'replay_system_track_link',
    'replay_system_track_mission',
    'replay_track_general_setting',
    'replay_ais_data'
]

REALTIME_STATE = {
    "cached_data": [],
    "data_time": [],
    "removed_data": [],
    "existed_data": [],
}

TACTICAL_FIGURE_STATE = {
    "cached_data": [],
    "removed_data": [],
    "existed_data": [],
}

REFERENCE_POINT_STATE = {
    "cached_data": [],
    "removed_data": [],
    "existed_data": [],
}

AREA_ALERT_STATE = {
    "cached_data": [],
    "removed_data": [],
    "existed_data": [],
}

SESSION_STATE = {
    "cached_data": [],
    "existed_data": [],
}

USERS = set()

async def send_cached_data(user, states=[]):
    # send realtime
    # np.array untuk mengambil index ke 1 dari semua row cached data
    for STATE in states:
        if len(STATE["cached_data"]) > 0:
            cached_data = np.array(STATE["cached_data"])
            message = list(cached_data[:, 1])
            message = json.dumps(message, default=str)
            await user.send(message)

async def register(websocket):
    USERS.add(websocket)
    await send_cached_data(websocket, states=[
        REALTIME_STATE,
        TACTICAL_FIGURE_STATE,
        REFERENCE_POINT_STATE,
        AREA_ALERT_STATE,
        SESSION_STATE,
    ])
    print(USERS)

async def unregister(websocket):
    USERS.remove(websocket)

async def data_change_detection():
    while True:
        # shiptrack data ------------------------------------------------------------------------
        shiptrack_data = np.array(information_data())
        await data_processing(shiptrack_data, REALTIME_STATE, USERS, data_category="realtime track", 
                        mandatory_attr="track_phase_type", 
                        must_remove=["DELETED_BY_SYSTEM", "DELETED_BY_SENSOR"], debug=True)

        # tactical figures ------------------------------------------------------------------------
        tactical_figure_datas = np.array(tactical_figure_data())
        await data_processing(tactical_figure_datas, TACTICAL_FIGURE_STATE, USERS, data_category="tactical figure", 
                                mandatory_attr="visibility_type", must_remove=["REMOVE"], debug=True)

        # reference points ------------------------------------------------------------------------
        reference_point_datas = np.array(reference_point_data())
        await data_processing(reference_point_datas, REFERENCE_POINT_STATE, USERS, data_category="reference point", 
                                mandatory_attr="visibility_type", must_remove=["REMOVE"], debug=True)

        # area alerts ------------------------------------------------------------------------
        area_alert_datas = np.array(area_alert_data())
        await data_processing(area_alert_datas, AREA_ALERT_STATE, USERS, data_category="area alerts", 
                                mandatory_attr="is_visible", must_remove=["REMOVE"], debug=True)

        # sessions ------------------------------------------------------------------------
        session_datas = np.array(session_data())
        await non_strict_data_processing(session_datas, SESSION_STATE, USERS, data_category="sessions", 
                                debug=True)
        print('========================================================================================================================')
        print('========================================================================================================================')
        # lama tidur
        await asyncio.sleep(3)

async def handler(websocket, path):
    await register(websocket),
    try:
        async for message in websocket:
            pass
    except websockets.exceptions.ConnectionClosedError:
        print("connection error")
    finally:
        await unregister(websocket)

# start_server = websockets.serve(handler, "192.168.43.14", 14045)
start_server = websockets.serve(handler, "127.0.0.1", 8080) 

tasks = [
    asyncio.ensure_future(data_change_detection()),
    asyncio.ensure_future(start_server)
]

asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks))
asyncio.get_event_loop().run_forever()
