#!/usr/bin/env python

# WS server example that synchronizes REALTIME_state across clients

import asyncio
import json
import logging
import websockets
import numpy as np
from tracker.models import information_data, tactical_figure_data, reference_point_data, \
                            area_alert_data, session_data, improved_track_data, history_dots
from tracker.actions import data_processing, non_strict_data_processing, send_history_dot
from tracker.config import WS_HOST, WS_PORT, r
from tracker.state import *
import tracker.util as util

logging.basicConfig()

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
    "existed_data_count": 0,
}

HISTORY_DOT_STATE = {
    "existed_data_count": 0,
}

async def send_cached_data(user, states=[]):
    # send realtime
    # np.array untuk mengambil index ke 1 dari semua row cached data
    data = dict()
    for STATE in states:
        if len(STATE[1]["cached_data"]) > 0:
            cached_data = np.array(STATE[1]["cached_data"])
            data[STATE[0]] = list(cached_data[:, 1])
        else:
            data[STATE[0]] = list()

    data['tracks'] = enhanced_send_track_cache()  # ambil data track yang berlaku di memory (enhanced)

    if len(data) > 0:
        message = json.dumps({'data': data, 'data_type': 'realtime'}, default=str)
        await user.send(message)

async def register(websocket):
    USERS.add(websocket)
    await send_cached_data(websocket, states=[
        ['tactical_figure', TACTICAL_FIGURE_STATE],
        ['reference_point', REFERENCE_POINT_STATE],
        ['area_alert', AREA_ALERT_STATE],
        ['session', SESSION_STATE],
    ])
    print("1 USER JOINED", websocket)
    print("TOTAL JOINED USER", len(USERS))

async def unregister(websocket):
    USERS.remove(websocket)
    print("1 USER LEFT", websocket)
    print("REMAINING USER", len(USERS))

def check_if_state_must_be_emptied(states):
    if SESSION_STATE['existed_data_count'] == 0:
        SESSION_STATE['existed_data_count'] = len(SESSION_STATE['existed_data'])

    # kalo misal sessionnya nambah, maka kosongkan data memory
    if SESSION_STATE['existed_data_count'] < len(SESSION_STATE['existed_data']):
        print("MENGOSONGKAN SESI")
        track_empty_memory()
        for state in states:
            for key in state.keys():
                state[key] = []
        SESSION_STATE['existed_data_count'] = len(SESSION_STATE['existed_data'])

async def data_change_detection():
    while True:
        # mengecek apakah data cached tersebut butuh dikosongkan
        # akan dikosongkan ketika sesi berganti
        check_if_state_must_be_emptied([AREA_ALERT_STATE])

        await improved_track_data() # get data track (enhanced)

        # tactical figures ------------------------------------------------------------------------
        tactical_figure_datas = np.array(tactical_figure_data())
        await data_processing(tactical_figure_datas, TACTICAL_FIGURE_STATE, USERS, NON_REALTIME_USERS, data_category="tactical_figure",
                                mandatory_attr="visibility_type", must_remove=["REMOVE"], debug=False)

        # reference points ------------------------------------------------------------------------
        reference_point_datas = np.array(reference_point_data())
        await data_processing(reference_point_datas, REFERENCE_POINT_STATE, USERS, NON_REALTIME_USERS, data_category="reference_point",
                                mandatory_attr="visibility_type", must_remove=["REMOVE"], debug=False)

        # area alerts ------------------------------------------------------------------------
        area_alert_datas = np.array(area_alert_data())
        await data_processing(area_alert_datas, AREA_ALERT_STATE, USERS, NON_REALTIME_USERS, data_category="area_alert",
                                mandatory_attr="is_visible", must_remove=["REMOVE"], debug=False)

        # sessions ------------------------------------------------------------------------
        session_datas = np.array(session_data())
        await non_strict_data_processing(session_datas, SESSION_STATE, USERS, NON_REALTIME_USERS, data_category="session",
                                debug=False)
        # lama tidur
        await asyncio.sleep(3)

async def get_websocket_messages(websocket):
    async for message in websocket:
        data = json.loads(message)
        print({'user': websocket, 'data': data})
        if 'action' in data:
            if data['action'] == 'realtime':
                await realtime_toggle_handler(websocket, data['action'])
                print('realtime')
            elif data['action'] == 'replay':
                await realtime_toggle_handler(websocket, data['action'])
                print('replay')
        if 'request_dots' in data:
            await send_history_dot(data['request_dots'], websocket)

async def realtime_toggle_handler(user, state):
    if state == 'realtime' and \
        user in NON_REALTIME_USERS:
        NON_REALTIME_USERS.remove(user)
        USERS.add(user)
        await send_cached_data(user, states=[
            ['tactical_figure', TACTICAL_FIGURE_STATE],
            ['reference_point', REFERENCE_POINT_STATE],
            ['area_alert', AREA_ALERT_STATE],
            ['session', SESSION_STATE],
        ])
    elif state == 'replay' and \
        user in USERS:
        USERS.remove(user)
        NON_REALTIME_USERS.add(user)


# =================================================================================
# IMPROVED SYSTEM
# =================================================================================
def enhanced_send_track_cache():
    # send realtime
    tracks = util.redis_decode_to_dict(r.hgetall('tracks'), nested_dict=True)
    completed_tracks = []

    for key, data in tracks.items():
        data['history_dots'] = history_dots(key)
        if r.exists('T' + key): completed_tracks.append(data)

    return completed_tracks

def track_empty_memory():
    r.flushdb()

# =================================================================================
# END IMPROVED SYSTEM
# =================================================================================

async def handler(websocket, path):
    print("WEBSOCKET STARTED")
    try:
        # -- event yang harus di jalankan oleh web socket --

        # meregister user ketika terkoneksi dengan web socket
        await register(websocket)

        # menghendle message dari client
        await get_websocket_messages(websocket)

    except websockets.exceptions.ConnectionClosedOK as e:
        print("connection closed ok but error", e)

    except websockets.exceptions.ConnectionClosedError as e:
        print("connection closed error", e)

    finally:
        await unregister(websocket)

def _main_():
    # 0.0.0.0 for global ip
    print("Server Started on " + WS_HOST + ":" + WS_PORT)
    start_server = websockets.serve(handler, WS_HOST, WS_PORT)

    tasks = [
        asyncio.ensure_future(data_change_detection()),
        asyncio.ensure_future(start_server)
    ]

    asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks))
    asyncio.get_event_loop().run_forever()
