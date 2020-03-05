#!/usr/bin/env python

# WS server example that synchronizes REALTIME_state across clients

import asyncio
import json
import logging
import websockets
import numpy as np
import sys
import os
from tracker.models import information_data, tactical_figure_data, reference_point_data, \
                            area_alert_data, session_data, replay_data, history_dots
from tracker.actions import data_processing, non_strict_data_processing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.config import WS_HOST, WS_PORT

logging.basicConfig()

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

TRACK_STATE = {
    "cached_data": [],
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
    "existed_data_count": 0,
}

# variable penyimpanan realtime user
USERS = set()

# variable penyimpanan non realtime user
NON_REALTIME_USERS = set()

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
    if len(data) > 0:
        message = json.dumps({'data': data, 'data_type': 'realtime'}, default=str)
        await user.send(message)

async def register(websocket):
    USERS.add(websocket)
    await send_cached_data(websocket, states=[
        ['track', TRACK_STATE],
        ['tactical_figure', TACTICAL_FIGURE_STATE],
        ['reference_point', REFERENCE_POINT_STATE],
        ['area_alert', AREA_ALERT_STATE],
        ['session', SESSION_STATE],
    ])
    print(USERS)

async def unregister(websocket):
    USERS.remove(websocket)

def check_if_state_must_be_emptied(states):
    if SESSION_STATE['existed_data_count'] == 0:
        print("***********************************NGISI*******************************************")
        SESSION_STATE['existed_data_count'] = len(SESSION_STATE['existed_data'])

    # kalo misal sessionnya nambah, maka kosongkan data memory
    if SESSION_STATE['existed_data_count'] < len(SESSION_STATE['existed_data']):
        print("***********************************KOSONGKANN*******************************************")
        for state in states:
            for key in state.keys():
                state[key] = []
        SESSION_STATE['existed_data_count'] = len(SESSION_STATE['existed_data'])

async def data_change_detection():
    while True:
        # mengecek apakah data cached tersebut butuh dikosongkan
        # akan dikosongkan ketika sesi berganti
        check_if_state_must_be_emptied([TRACK_STATE, AREA_ALERT_STATE])

        # shiptrack data ------------------------------------------------------------------------
        shiptrack_data = np.array(information_data())
        await data_processing(shiptrack_data, TRACK_STATE, USERS, NON_REALTIME_USERS, data_category="track",
                        mandatory_attr="track_phase_type", must_remove=["DELETED_BY_SYSTEM", "DELETED_BY_SENSOR"], debug=True)

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
        print('========================================================================================================================')
        print('========================================================================================================================')
        # lama tidur
        await asyncio.sleep(3)

async def send_replay_track(session, user):
    print(session, " send to ", user)
    data = replay_data(session)

    message = json.dumps({'data': data, 'data_type': 'replay'}, default=str)
    await user.send(message)

async def send_history_dot(system_track_number, user):
    print(system_track_number, " send to ", user)
    data = history_dots(system_track_number)

    message = json.dumps({'data': data, 'data_type': 'history_dots', 'system_track_number': system_track_number}, default=str)
    await user.send(message)

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
        if 'request' in data:
            await send_replay_track(data['request'], websocket)
        if 'request_dots' in data:
            await send_history_dot(data['request_dots'], websocket)

async def realtime_toggle_handler(user, state):
    if state == 'realtime' and \
        user in NON_REALTIME_USERS:
        NON_REALTIME_USERS.remove(user)
        USERS.add(user)
        await send_cached_data(user, states=[
            ['track', TRACK_STATE],
            ['tactical_figure', TACTICAL_FIGURE_STATE],
            ['reference_point', REFERENCE_POINT_STATE],
            ['area_alert', AREA_ALERT_STATE],
            ['session', SESSION_STATE],
        ])
    elif state == 'replay' and \
        user in USERS:
        USERS.remove(user)
        NON_REALTIME_USERS.add(user)

async def handler(websocket, path):
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
    start_server = websockets.serve(handler, WS_HOST, WS_PORT)

    tasks = [
        asyncio.ensure_future(data_change_detection()),
        asyncio.ensure_future(start_server)
    ]

    asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks))
    asyncio.get_event_loop().run_forever()
