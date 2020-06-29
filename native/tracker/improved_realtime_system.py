#!/usr/bin/env python

# WS server example that synchronizes REALTIME_state across clients

import asyncio
import json
import logging
import websockets
import threading
from multiprocessing import Process
import numpy as np
import sys
import pickle
from tracker.models import information_data, tactical_figure_data, reference_point_data, \
                            area_alert_data, session_data, improved_track_data, history_dots
from tracker.actions import data_processing, non_strict_data_processing, send_history_dot
from tracker.config import WS_HOST, WS_PORT, r, r_channel
from tracker.state import USERS, NON_REALTIME_USERS
from tracker import state
import tracker.util as util

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger()
# here
logger.setLevel(logging.INFO)

async def send_cached_data(user, states=[]):
    # send realtime
    # np.array untuk mengambil index ke 1 dari semua row cached data
    data = dict()
    for STATE in states:
        if len(STATE[1]["cached_data"]) > 0:
            cached_data = np.array(STATE[1]["cached_data"])
            data[STATE[0]] = list(cached_data)
        else:
            data[STATE[0]] = list()

    data['tracks'] = enhanced_send_track_cache()  # ambil data track yang berlaku di memory (enhanced)

    if len(data) > 0:
        message = json.dumps({'data': data, 'data_type': 'realtime'}, default=str)
        await user.send(message)

class StoreWebsocket(object):
    def __init__(self, websocket):
        self.websocket = websocket

    def getSockets(self):
        return self.websocket

async def register(websocket):
    TACTICAL_FIGURE_STATE = {
        "cached_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "TACTICAL_FIGURE_C_*")
        ],
        "removed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "TACTICAL_FIGURE_R_*")
        ],
        "existed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "TACTICAL_FIGURE_E_*")
        ],
    }
    REFERENCE_POINT_STATE = {
        "cached_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "REFERENCE_POINT_C_*")
        ],
        "removed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "REFERENCE_POINT_R_*")
        ],
        "existed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "REFERENCE_POINT_E_*")
        ],
    }
    AREA_ALERT_STATE = {
        "cached_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "AREA_ALERT_C_*")
        ],
        "removed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "AREA_ALERT_R_*")
        ],
        "existed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "AREA_ALERT_E_*")
        ],
    }
    SESSION_STATE = {
        "cached_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "SESSION_C_*")
        ],
        "existed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "SESSION_E_*")
        ],
    }

    # str_object = str(websocket)
    # str_object = str_object[1:]
    # str_object = str_object[:-1]
    # websocket_id = str_object.split()[3]

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
    session_data_count = r.get('session_data_count')
    existed_data_count = session_data_count.decode() if session_data_count != None else 0
    existed_data_count = int(existed_data_count)
    existed_data = util.redis_scan_keys(r, "SESSION_E_")

    if existed_data_count == 0:
        existed_data_count = len(existed_data)

    # kalo misal sessionnya nambah, maka kosongkan data memory
    elif existed_data_count < len(existed_data):
        await must_notify_user_on_session_finished()

        print("MENGOSONGKAN SESI")
        track_empty_memory()
        for state in states:
            for key in state.keys():
                state[key] = []
        existed_data_count = len(existed_data)

    r.set('session_data_count', existed_data_count)

async def must_notify_user_on_session_finished():
    print("KUDUNE MASUK")
    if USERS:
        send_data = dict()
        send_data['finished'] = True
        message = json.dumps({'data': send_data, 'data_type': 'session'}, default=str)
        await asyncio.wait([user.send(message) for user in USERS])

async def data_change_detection():
    TACTICAL_FIGURE_STATE = {
        "cached_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "TACTICAL_FIGURE_C_*")
        ],
        "removed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "TACTICAL_FIGURE_R_*")
        ],
        "existed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "TACTICAL_FIGURE_E_*")
        ],
    }
    REFERENCE_POINT_STATE = {
        "cached_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "REFERENCE_POINT_C_*")
        ],
        "removed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "REFERENCE_POINT_R_*")
        ],
        "existed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "REFERENCE_POINT_E_*")
        ],
    }
    AREA_ALERT_STATE = {
        "cached_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "AREA_ALERT_C_*")
        ],
        "removed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "AREA_ALERT_R_*")
        ],
        "existed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "AREA_ALERT_E_*")
        ],
    }
    SESSION_STATE = {
        "cached_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "SESSION_C_*")
        ],
        "existed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "SESSION_E_*")
        ],
    }
    while True:
        if not state.DATA_READY: logging.info('Reading Database...\nPlease Wait Until Database Ready!')
        if not state.FINISHED_CHECK:
            if state.DATA_READY:
                logging.info('Database Ready!')
                state.FINISHED_CHECK = True

        # mengecek apakah data cached tersebut butuh dikosongkan
        # akan dikosongkan ketika sesi berganti
        await check_if_state_must_be_emptied([AREA_ALERT_STATE])

        # tactical figures ------------------------------------------------------------------------
        tactical_figure_datas = np.array(tactical_figure_data())
        await data_processing(tactical_figure_datas, TACTICAL_FIGURE_STATE, USERS, NON_REALTIME_USERS, data_category="tactical_figure",
                                mandatory_attr="visibility_type", must_remove=["REMOVE"], debug=False)
        if not state.DATA_READY: logging.info('Preparing Tactical Figures!')

        # reference points ------------------------------------------------------------------------
        reference_point_datas = np.array(reference_point_data())
        await data_processing(reference_point_datas, REFERENCE_POINT_STATE, USERS, NON_REALTIME_USERS, data_category="reference_point",
                                mandatory_attr="visibility_type", must_remove=["REMOVE"], debug=False)
        if not state.DATA_READY: logging.info('Preparing Reference Points!')

        # area alerts ------------------------------------------------------------------------
        area_alert_datas = np.array(area_alert_data())
        await data_processing(area_alert_datas, AREA_ALERT_STATE, USERS, NON_REALTIME_USERS, data_category="area_alert",
                                mandatory_attr="is_visible", must_remove=["REMOVE"], debug=False)
        if not state.DATA_READY: logging.info('Preparing Area Alerts!')

        # sessions ------------------------------------------------------------------------
        session_datas = np.array(session_data())
        await non_strict_data_processing(session_datas, SESSION_STATE, USERS, NON_REALTIME_USERS, data_category="session",
                                debug=False)
        if not state.DATA_READY: logging.info('Preparing Sessions!')

        util.write_array_to_redis(r, ["TACTICAL_FIGURE_C_", TACTICAL_FIGURE_STATE["cached_data"]])
        util.write_int_to_redis(r, ["TACTICAL_FIGURE_R_", TACTICAL_FIGURE_STATE["removed_data"]])
        util.write_int_to_redis(r, ["TACTICAL_FIGURE_E_", TACTICAL_FIGURE_STATE["existed_data"]])
        util.write_array_to_redis(r, ["REFERENCE_POINT_C_", REFERENCE_POINT_STATE["cached_data"]])
        util.write_int_to_redis(r, ["REFERENCE_POINT_R_", REFERENCE_POINT_STATE["removed_data"]])
        util.write_int_to_redis(r, ["REFERENCE_POINT_E_", REFERENCE_POINT_STATE["existed_data"]])
        util.write_array_to_redis(r, ["AREA_ALERT_C_", AREA_ALERT_STATE["cached_data"]])
        util.write_int_to_redis(r, ["AREA_ALERT_R_", AREA_ALERT_STATE["removed_data"]])
        util.write_int_to_redis(r, ["AREA_ALERT_E_", AREA_ALERT_STATE["existed_data"]])
        util.write_array_to_redis(r, ["SESSION_C_", SESSION_STATE["cached_data"]])
        util.write_int_to_redis(r, ["SESSION_E_", SESSION_STATE["existed_data"]])

        await improved_track_data() # get data track (enhanced)
        if not state.DATA_READY: logging.info('Preparing Tracks!')

        state.DATA_READY = True
        try:
            # lama tidur
            await asyncio.sleep(0.5)
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
    TACTICAL_FIGURE_STATE = {
        "cached_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "TACTICAL_FIGURE_C_*")
        ],
        "removed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "TACTICAL_FIGURE_R_*")
        ],
        "existed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "TACTICAL_FIGURE_E_*")
        ],
    }
    REFERENCE_POINT_STATE = {
        "cached_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "REFERENCE_POINT_C_*")
        ],
        "removed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "REFERENCE_POINT_R_*")
        ],
        "existed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "REFERENCE_POINT_E_*")
        ],
    }
    AREA_ALERT_STATE = {
        "cached_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "AREA_ALERT_C_*")
        ],
        "removed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "AREA_ALERT_R_*")
        ],
        "existed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "AREA_ALERT_E_*")
        ],
    }
    SESSION_STATE = {
        "cached_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "SESSION_C_*")
        ],
        "existed_data": [
              json.loads(r.get(i.decode()).decode()) if len(i) > 0 else [] for i in util.redis_scan_keys(r, "SESSION_E_*")
        ],
    }
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
    redis_keys = util.redis_scan_keys(r, 'TRACK_*')
    # send realtime
    completed_tracks = []

    for k in redis_keys:
        decoded_k = k.decode()
        data = json.loads(r.get(decoded_k).decode())
        data['system_track_number'] = int(decoded_k[6:])
        completed_tracks.append(data)

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

def websocket_server_handler():
    # 0.0.0.0 for global ip
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    print("Server Started on " + WS_HOST + ":" + WS_PORT)
    start_server = websockets.serve(handler, WS_HOST, WS_PORT)
    try:
        loop.run_until_complete(start_server)
        loop.run_forever()
    except Exception as e:
        print(e)

def data_checker_handler():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # try:
    loop.run_until_complete(data_change_detection())
    loop.run_forever()
    # except Exception as e:
    #     print(e)

def _main_():
    try:
        # multiprocessing
        ws_thread = Process(name='ws_thread', target=websocket_server_handler)
        data_checker = Process(name='data_checker', target=data_checker_handler)

        ws_thread.daemon = True
        data_checker.daemon = True

        # multithreading
        # ws_thread = threading.Thread(tas

        ws_thread.start()
        data_checker.start()

        # seems simple, but trust me it works a lot!
        while True: pass

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