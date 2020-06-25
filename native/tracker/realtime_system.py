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
from tracker.state import USERS, NON_REALTIME_USERS
from tracker import state
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
    print("1 USER JOINED, TOTAL JOINED USER", len(USERS))

async def unregister(websocket):
    USERS.remove(websocket)
    print("1 USER LEFT, REMAINING USER", len(USERS))

async def check_if_state_must_be_emptied(states):
    if SESSION_STATE['existed_data_count'] == 0:
        SESSION_STATE['existed_data_count'] = len(SESSION_STATE['existed_data'])

    # kalo misal sessionnya nambah, maka kosongkan data memory
    if SESSION_STATE['existed_data_count'] < len(SESSION_STATE['existed_data']):
        await must_notify_user_on_session_finished()

        print("MENGOSONGKAN SESI")
        track_empty_memory()
        for state in states:
            for key in state.keys():
                state[key] = []
        SESSION_STATE['existed_data_count'] = len(SESSION_STATE['existed_data'])

async def must_notify_user_on_session_finished():
    print("KUDUNE MASUK")
    if USERS:
        send_data = dict()
        send_data['finished'] = True
        message = json.dumps({'data': send_data, 'data_type': 'session'}, default=str)
        await asyncio.wait([user.send(message) for user in USERS])

async def data_change_detection():
    while True:
        if not state.DATA_READY: print("\nReading Database...\nPlease Wait Until Database Ready!")
        if not state.FINISHED_CHECK:
            if state.DATA_READY:
                print("Database Ready!")
                state.FINISHED_CHECK = True

        # mengecek apakah data cached tersebut butuh dikosongkan
        # akan dikosongkan ketika sesi berganti
        await check_if_state_must_be_emptied([AREA_ALERT_STATE])

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
        state.DATA_READY = True
        try:
            # lama tidur
            await asyncio.sleep(3)
        except asyncio.CancelledError:
            break

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
        data['system_track_number'] = int(key)
        # data['history_dots'] = json.loads(r.hget('history_dots', key).decode("utf-8")) \
        #                             if r.hexists('history_dots', key) else []
        if r.exists('T' + key): completed_tracks.append(data)

    return completed_tracks

def track_empty_memory():
    print("REDIS TRACK KEYS - BEFORE CLEAR", r.exists("tracks"))
    print("REDIS HISTORY DOTS KEYS - BEFORE CLEAR", r.exists("history_dots"))
    r.flushdb()
    print("REDIS TRACK KEYS - AFTER CLEAR", r.exists("tracks"))
    print("REDIS HISTORY DOTS KEYS - AFTER CLEAR", r.exists("history_dots"))

# =================================================================================
# END IMPROVED SYSTEM
# =================================================================================

async def handler(websocket, path):
    print("HANDSHAKED")
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

    loop = asyncio.get_event_loop()
    try:
        future = asyncio.gather(*tasks)
        loop.run_until_complete(future)
        loop.run_forever()
    except Exception as e:
        print(e)
    except asyncio.CancelledError:
        print('Tasks has been canceled')
    except KeyboardInterrupt:
        print("Exitting...")
    finally:
        print('Stopping')
        for task in asyncio.Task.all_tasks():
            task.cancel()
        # future.cancel()
        # loop.stop()
        # loop.close()
