#!/usr/bin/env python

# WS server example that synchronizes state across clients

import asyncio
import json
import logging
import websockets
import random
import psycopg2
import numpy as np
from functools import reduce

logging.basicConfig()

conn = psycopg2.connect("host=localhost dbname=shiptrack user=postgres password=bismillah")
# conn = psycopg2.connect("host=localhost dbname=shiptrack user=postgres password=bismillah")
cur = conn.cursor()
# i = 0;
ar_mandatory_table = [
    'replay_system_track_general',
    'replay_system_track_kinetic',
    'replay_system_track_processing',
    'replay_track_general_setting'
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

# array untk menyimpan track number dari mandatory table
ar_dis_track_number_mandatory_table = [[], [], [], []]

# untuk menyimpan isi panjangnya
# kalau panjangnya sudah sama berarti lengkap
ar_dis_track_number_mandatory_table_pjg = [0, 0, 0, 0]

# panjang
ar_dis_track_number_mandatory_table_pjg_cr_time = ['-', '-', '-', '-']

# untuk menyimpan create untuk masing - masing 8
# tabel
last_system_track_number_kirim_datetime = []

# menyimpan sistem track yang sudah dikirim
last_system_track_number_kirim = ['-', '-', '-', '-', '-', '-', '-', '-']

# array untuk menyimpan track_number dari setiap
# mandatory table
# [
#     [1, 2, 3, 4],
#     [5, 6, 7, 8],
#     [9, 0, 11, 12],
# ]
STATE = {
    "mandatory_datas": [],
    "completed_data": [],
    "sent_data": [],
}

USERS = set()


def information_data():
    prepared_data = []
    try:
        # untuk menyimpan create untuk masing - masing 8
        # tabel
        last_system_track_number_kirim_datetime = [
            '0000-00-00 00:00:00',
            '0000-00-00 00:00:00',
            '0000-00-00 00:00:00',
            '0000-00-00 00:00:00',
            '0000-00-00 00:00:00',
            '0000-00-00 00:00:00',
            '0000-00-00 00:00:00',
            '0000-00-00 00:00:00'
        ]
        data_ready = []
        # 1. ambil distinct data
        # 1.1. looping per mandatory table

        # while True:
        mandatory_table_system_track_numbers = []
        for ix, table_name_ in enumerate(ar_mandatory_table):
            if (ar_mandatory_table[ix] == 'replay_system_track_general'):
                q = "SELECT " \
                    "    st.system_track_number," \
                    "    mx.created_time," \
                    "    st.session_id " \
                    "FROM {table_name} st " \
                    "JOIN sessions s ON st.session_id = s.id " \
                    "JOIN " \
                    "(" \
                    "    SELECT " \
                    "        session_id," \
                    "        system_track_number," \
                    "        max(created_time) created_time " \
                    "    FROM {table_name} " \
                    "    GROUP BY session_id,system_track_number" \
                    ") mx ON st.system_track_number = mx.system_track_number " \
                    "and st.created_time=mx.created_time " \
                    "and st.session_id=mx.session_id " \
                    "WHERE s.end_time is NULL " \
                    "AND st.own_unit_indicator='FALSE' " \
                    "ORDER BY st.system_track_number;" \
                .format(table_name=ar_mandatory_table[ix])
            else:
                q = "SELECT " \
                    "    st.system_track_number," \
                    "    mx.created_time," \
                    "    st.session_id " \
                    "FROM {table_name} st " \
                    "JOIN sessions s ON st.session_id = s.id " \
                    "JOIN " \
                    "(" \
                    "    SELECT " \
                    "        session_id," \
                    "        system_track_number," \
                    "        max(created_time) created_time " \
                    "    FROM {table_name} " \
                    "    GROUP BY session_id,system_track_number" \
                    ") mx ON st.system_track_number = mx.system_track_number " \
                    "and st.created_time = mx.created_time " \
                    "and st.session_id = mx.session_id " \
                    "WHERE s.end_time is NULL " \
                    "ORDER BY st.system_track_number;" \
                .format(table_name=ar_mandatory_table[ix])
            cur.execute(q)
            data = cur.fetchall()

            table_data = []
            for idx, row in enumerate(data):
                table_data.append(row[0])
                session_id = row[2]
            mandatory_table_system_track_numbers.append(table_data)
        # system tracking number disimpan ke arr STATE["mandatory_datas"]
        STATE["mandatory_datas"] = np.array(mandatory_table_system_track_numbers)

        # data ready adalah data yang sudah diintersect untuk mengambil data yang sama
        data_ready = reduce(np.intersect1d, STATE["mandatory_datas"])

        # 2. kirim data lengkap
        ship_tracks = []
        # loop = 0
        for ready in data_ready:
            # loop += 1
            # if loop == 5:
            #     break
            columns = ('system_track_number','created_time','identity','environment','source','track_name','iu_indicator','airborne_indicator')
            results = []
            for ix in range(len(ar_mandatory_table_8)):
                #dapatkan created time yang terakhir per 8 tabel tersebut
                q = "SELECT " \
                    "   max(created_time) created_time " \
                    "FROM " + ar_mandatory_table_8[ix] +" " \
                    "WHERE session_id = " + str(session_id) + " " \
                    "AND system_track_number = " + str(ready) + ";"
                cur.execute(q)
                data = cur.fetchall()
                for row in data:
                    created_time = str(row[0])

                if(created_time>str(last_system_track_number_kirim_datetime[ix])):
                    # kirimkan data dengan created time terbaru
                    # dan simpan ke last_system_track_number_kirim
                    last_system_track_number_kirim_datetime[ix] = created_time
                if(ar_mandatory_table_8[ix]=='replay_system_track_general'):
                    q = "SELECT " \
                        "   system_track_number," \
                        "   created_time,identity," \
                        "   environment," \
                        "   source," \
                        "   track_name," \
                        "   iu_indicator," \
                        "   airborne_indicator " \
                        "FROM " + ar_mandatory_table_8[ix] + " " \
                        "WHERE session_id = " + str(session_id) + " " \
                        "AND system_track_number = " + str(ready) + " " \
                        "AND created_time = '" + last_system_track_number_kirim_datetime[ix] + "';"
                    cur.execute(q)
                    for row in cur.fetchall():
                        results.append(dict(zip(columns, row)))
                if(ar_mandatory_table_8[ix]=='replay_system_track_kinetic'):
                    q = "SELECT " \
                        "   latitude," \
                        "   longitude," \
                        "   speed_over_ground," \
                        "   course_over_ground " \
                        "FROM " + ar_mandatory_table_8[ix] + " " \
                        "WHERE session_id = " + str(session_id) + " " \
                        "AND system_track_number = " + str(ready) + " " \
                        "AND created_time = '" + last_system_track_number_kirim_datetime[ix]+ "';"
                    cur.execute(q)
                    for row in cur.fetchall():
                        results[len(results)-1]['latitude'] = row[0]
                        results[len(results)-1]['longitude'] = row[1]
                        results[len(results)-1]['speed_over_ground'] = row[2]
                        results[len(results)-1]['course_over_ground'] = row[3]

                if(ar_mandatory_table_8[ix]=='replay_system_track_processing'):
                    q = "SELECT " \
                        "   track_join_status," \
                        "   track_fusion_status," \
                        "   track_phase_type as track_phase " \
                        "FROM " + ar_mandatory_table_8[ix] + " " \
                        "WHERE session_id = " + str(session_id) + " " \
                        "AND system_track_number = " + str(ready) + " " \
                        "AND created_time = '" + last_system_track_number_kirim_datetime[ix] + "';"
                    cur.execute(q)
                    for row in cur.fetchall():
                        results[len(results)-1]['track_join_status'] = row[0]
                        results[len(results)-1]['track_fusion_status'] = row[1]
                        results[len(results)-1]['track_phase'] = row[2]

                if(ar_mandatory_table_8[ix]=='replay_ais_data'):
                    icek_ais = 0
                    if(last_system_track_number_kirim_datetime[ix]!='None'):
                        q = "SELECT " \
                            "   type_of_ship_or_cargo," \
                            "   name as ship_name " \
                            "FROM " + ar_mandatory_table_8[ix] + " " \
                            "WHERE session_id = " + str(session_id) + " " \
                            "AND system_track_number = " + str(ready) + " " \
                            "AND created_time = '" + last_system_track_number_kirim_datetime[ix] + "';"
                        cur.execute(q)
                        for row in cur.fetchall():
                            results[len(results)-1]['type_of_ship_or_cargo'] = row[0]
                            results[len(results)-1]['ship_name'] = row[1]
                            icek_ais = 1

                    else:
                        q = "SELECT " \
                            "   * " \
                            "FROM " \
                            "(" \
                            "   SELECT " \
                            "       type_of_ship_or_cargo," \
                            "       name as ship_name " \
                            "   FROM " + ar_mandatory_table_8[ix] +" " \
                            "   WHERE session_id = " + str(session_id) +" " \
                            "   AND system_track_number = " + str(ready) +" " \
                            "   ORDER BY created_time DESC " \
                            ") aa LIMIT 1;"
                        cur.execute(q)
                        for row in cur.fetchall():
                            results[len(results)-1]['type_of_ship_or_cargo'] = row[0]
                            results[len(results)-1]['ship_name'] = row[1]
                            icek_ais = 1

                    if(icek_ais == 0):
                        results[len(results)-1]['type_of_ship_or_cargo'] = '-'
                        results[len(results)-1]['ship_name'] = '-'

                if(ar_mandatory_table_8[ix]=='replay_track_general_setting'):
                    q = "SELECT " \
                        "   track_visibility " \
                        "FROM " + ar_mandatory_table_8[ix] + " " \
                        "WHERE session_id = " + str(session_id) + " " \
                        "AND system_track_number = " + str(ready) + " " \
                        "AND created_time = '" + last_system_track_number_kirim_datetime[ix] + "';"
                    cur.execute(q)
                    for row in cur.fetchall():
                        results[len(results)-1]['track_visibility'] = row[0]
            ship_tracks.extend(results)
        prepared_data.extend(ship_tracks)
        return prepared_data
        # return json.dumps(ship_tracks, default=str)
    except psycopg2.Error as e:
        print(e)
    # cur.close()
    # conn.close()

async def users_event():
    return json.dumps({"type": "users", "count": len(USERS)})

async def notify_users():
    if USERS:
        message = await users_event()
        await asyncio.wait([user.send(message) for user in USERS])

async def send_data():
    if USERS:
        if len(STATE["completed_data"]) == 0:
            STATE["completed_data"] = information_data()

        if len(STATE["sent_data"]) != len(STATE["completed_data"]):
            message = STATE["completed_data"]
            STATE["sent_data"].extend(STATE["completed_data"])
        else:
            message = STATE["sent_data"]
        message = json.dumps(message, default=str)
        await asyncio.wait([user.send(message) for user in USERS])

async def register(websocket):
    USERS.add(websocket)
    print(USERS)
    await notify_users()

async def unregister(websocket):
    USERS.remove(websocket)
    await notify_users()

async def detect_insertion_data():
    while True:
        if len(STATE["completed_data"]) == 0:
            STATE["completed_data"] = information_data()
        print("its looped!")
        await asyncio.sleep(3)

async def handler(websocket, path):
    await register(websocket),
    try:
        await send_data()

        async for message in websocket:
            pass
    finally:
        await unregister(websocket)

# start_server = websockets.serve(handler, "10.20.112.203", 8080)
start_server = websockets.serve(handler, "127.0.0.1", 8080)

tasks = [
    asyncio.ensure_future(detect_insertion_data()),
    asyncio.ensure_future(start_server)
]

asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks))
asyncio.get_event_loop().run_forever()
