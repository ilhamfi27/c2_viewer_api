#!/usr/bin/env python

# WS server example that synchronizes REALTIME_state across clients

import asyncio
import json
import logging
import websockets
import random
import psycopg2
import numpy as np
from functools import reduce

logging.basicConfig()

conn = psycopg2.connect("host=127.0.0.1 \
    dbname=shiptrack \
    user=postgres \
    password=bismillah"
)

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

USERS = set()


def information_data():
    try:
        mandatory_data = []
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
        # system tracking number disimpan ke arr mandatory_data
        mandatory_data = np.array(mandatory_table_system_track_numbers)

        # data ready adalah data yang sudah diintersect untuk mengambil data yang sama
        data_ready = reduce(np.intersect1d, mandatory_data)

        # 2. kirim data lengkap
        ship_tracks = []
        # loop = 0
        for ready in data_ready:
            # loop += 1
            # if loop == 5:
            #     break
            columns = ('system_track_number','created_time','identity','environment','source','track_name','iu_indicator','airborne_indicator')
            results = {}
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
                    q = "SELECT * FROM " \
                        "(  SELECT " \
                        "   system_track_number," \
                        "   created_time,identity," \
                        "   environment," \
                        "   source," \
                        "   track_name," \
                        "   iu_indicator " \
                        "FROM   " + ar_mandatory_table_8[ix] + " " \
                        "WHERE session_id = " + str(session_id) + " " \
                        "AND system_track_number = " + str(ready) + " ORDER BY created_time DESC) ss LIMIT 1"
                    cur.execute(q)
                    for row in cur.fetchall():
                        system_track_number = row[0]
                        results = dict(zip(columns, row))
                        source_data = row[4]
                if(ar_mandatory_table_8[ix]=='replay_system_track_kinetic'):
                    q = "SELECT * FROM" \
                        "( SELECT " \
                        "   latitude," \
                        "   longitude," \
                        "   speed_over_ground," \
                        "   course_over_ground " \
                        "FROM " + ar_mandatory_table_8[ix] + " " \
                        "WHERE session_id = " + str(session_id) + " " \
                        "AND system_track_number = " + str(ready) + " " \
                        "ORDER BY created_time DESC) aa LIMIT 1;"
                    cur.execute(q)
                    for row in cur.fetchall():
                        results['latitude'] = row[0]
                        results['longitude'] = row[1]
                        results['speed_over_ground'] = row[2]
                        results['course_over_ground'] = row[3]

                if(ar_mandatory_table_8[ix]=='replay_system_track_processing'):
                    q = "SELECT * FROM" \
                        "( SELECT " \
                        "   track_join_status," \
                        "   track_fusion_status," \
                        "   track_phase_type as track_phase " \
                        "FROM " + ar_mandatory_table_8[ix] + " " \
                        "WHERE session_id = " + str(session_id) + " " \
                        "AND system_track_number = " + str(ready) + " " \
                        "ORDER BY created_time DESC) aa LIMIT 1;"
                    cur.execute(q)
                    for row in cur.fetchall():
                        if len(results) > 0:
                            results['track_join_status'] = row[0]
                            results['track_fusion_status'] = row[1]
                            results['track_phase'] = row[2]

                if(ar_mandatory_table_8[ix]=='replay_ais_data'):
                    if(source_data=='AIS_TYPE'):
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
                            if len(results) > 0:
                                results['type_of_ship_or_cargo'] = row[0]
                                results['ship_name'] = row[1]
                    else:
                        if len(results) > 0:
                            results['type_of_ship_or_cargo'] = '-'
                            results['ship_name'] = '-'

                if(ar_mandatory_table_8[ix]=='replay_track_general_setting'):
                    q = "SELECT * FROM " \
                        "( SELECT " \
                        "   track_visibility " \
                        "FROM " + ar_mandatory_table_8[ix] + " " \
                        "WHERE session_id = " + str(session_id) + " " \
                        "AND system_track_number = " + str(ready) + " " \
                        "ORDER BY created_time DESC) aa LIMIT 1 ;"
                    cur.execute(q)
                    for row in cur.fetchall():
                        if len(results) > 0:
                            results['track_visibility'] = row[0]
            ship_tracks.append([system_track_number, results])
        return ship_tracks
    except psycopg2.Error as e:
        print(e)
    cur.close()
    conn.close()

def tactical_figure_data():
    try:
        columns = (
            "object_id", "object_type", "name", "environment", "shape", "displaying_popup_alert_status",
            "line_color", "fill_color", "identity_list", "warning_list", "evaluation_type", "visibility_type",
            "last_update_time", "network_track_number", "link_status_type", "is_editable", "point_amplification_type",
            "point_keys", "points"
        )

        q = "SELECT * " \
            "    FROM tactical_figure_list " \
            "    ORDER BY object_id; "
        cur.execute(q)
        data = []
        for row in cur.fetchall():
            object_id = row[0]
            results = dict(zip(columns, row))
            data.append([object_id, results])
        return data
    except psycopg2.Error as e:
        print(e)
    cur.close()
    conn.close()

def reference_point_data():
    try:
        columns = (
            "object_type", "object_id", "name", "latitude", "longitude", "altitude", "visibility_type",
            "point_amplification_type", "is_editable", "network_track_number", "link_status_type", "last_update_time",
        )

        q = "SELECT * " \
            "    FROM reference_points " \
            "    ORDER BY object_id; "
        cur.execute(q)
        data = []
        for row in cur.fetchall():
            object_id = row[0]
            results = dict(zip(columns, row))
            data.append([object_id, results])
        return data
    except psycopg2.Error as e:
        print(e)
    cur.close()
    conn.close()

def area_alert_data():

    try:
        columns = (
            "session_id", "object_type", "object_id", "warning_type",
            "track_name", "last_update_time", "mmsi_number", "ship_name",
            "track_source_type", "is_visible"
        )

        q = "SELECT aa.* " \
            "    FROM area_alerts aa " \
            "JOIN sessions s ON aa.session_id=s.id " \
            "JOIN " \
            "( " \
            "    SELECT session_id,object_id,max(last_update_time) last_update_time " \
            "    FROM area_alerts " \
            "    GROUP BY session_id,object_id " \
            ") mx ON aa.object_id=mx.object_id  " \
            "and aa.last_update_time=mx.last_update_time " \
            "and aa.session_id=mx.session_id " \
            "WHERE s.end_time is NULL " \
            "ORDER BY aa.object_id; "
        cur.execute(q)
        data = []
        for row in cur.fetchall():
            object_id = row[3]
            results = dict(zip(columns, row))
            data.append([object_id, results])
        return data
    except psycopg2.Error as e:
        print(e)
    cur.close()
    conn.close()

async def send_cached_data():
    if USERS:
        # send realtime
        message = REALTIME_STATE["cached_data"]
        message = json.dumps(message, default=str)
        await asyncio.wait([user.send(message) for user in USERS])

        # send tactical figure
        message = TACTICAL_FIGURE_STATE["cached_data"]
        message = json.dumps(message, default=str)
        await asyncio.wait([user.send(message) for user in USERS])

        # send reference point
        message = REFERENCE_POINT_STATE["cached_data"]
        message = json.dumps(message, default=str)
        await asyncio.wait([user.send(message) for user in USERS])

        # area alert point
        message = AREA_ALERT_STATE["cached_data"]
        message = json.dumps(message, default=str)
        await asyncio.wait([user.send(message) for user in USERS])

async def register(websocket):
    USERS.add(websocket)
    print(USERS)

async def unregister(websocket):
    USERS.remove(websocket)

async def data_change_detection():
    while True:
        # shiptrack data ------------------------------------------------------------------------
        shiptrack_data = np.array(information_data())

        datas_changed = 0
        if len(shiptrack_data) > 0: 
            check_track_number_system = np.setdiff1d(shiptrack_data[0:, 0], np.array(REALTIME_STATE["existed_data"]))
            if len(check_track_number_system) > 0:
                REALTIME_STATE["existed_data"].extend(check_track_number_system)
                REALTIME_STATE["cached_data"] = list(shiptrack_data[0:, 1])
                print("insert")

            if len(check_track_number_system) == 0:
                changed_data = []
                if len(REALTIME_STATE["cached_data"]) > 0:
                    for i, data_has_changed in enumerate(shiptrack_data[0:, 1]):
                        if REALTIME_STATE["cached_data"][i] != shiptrack_data[i, 1] and \
                            shiptrack_data[i, 0] not in REALTIME_STATE["removed_data"]:
                            changed_data.append(data_has_changed)
                            print("update")

                        elif REALTIME_STATE["cached_data"][i] != shiptrack_data[i, 1] and \
                            shiptrack_data[i, 0] not in REALTIME_STATE["removed_data"] and \
                            shiptrack_data[i, 1] == "REMOVE":
                            REALTIME_STATE["removed_data"].extend(shiptrack_data[i, 1])
                            del REALTIME_STATE["cached_data"][i]
                            changed_data.append(data_has_changed)
                            print("deleted")

                print("changed data", changed_data)
                if len(changed_data) > 0:
                    datas_changed += 1
                    REALTIME_STATE["cached_data"] = list(shiptrack_data[0:, 1])
                    if USERS:
                        message = json.dumps(changed_data, default=str)
                        await asyncio.wait([user.send(message) for user in USERS])

        # tactical figures ------------------------------------------------------------------------
        tactical_figure_datas = np.array(tactical_figure_data())

        if len(tactical_figure_datas) > 0: 
            check_object_id = np.setdiff1d(tactical_figure_datas[0:, 0], np.array(TACTICAL_FIGURE_STATE["existed_data"]))
            if len(check_object_id) > 0:
                TACTICAL_FIGURE_STATE["existed_data"].extend(check_object_id)
                TACTICAL_FIGURE_STATE["cached_data"] = list(tactical_figure_datas[0:, 1])

            if len(check_object_id) == 0:
                changed_data = []
                if len(TACTICAL_FIGURE_STATE["cached_data"]) > 0:
                    for i, data_has_changed in enumerate(tactical_figure_datas[0:, 1]):
                        if TACTICAL_FIGURE_STATE["cached_data"][i] != tactical_figure_datas[i, 1] and \
                            tactical_figure_datas[i, 0] not in TACTICAL_FIGURE_STATE["removed_data"]:
                            changed_data.append(data_has_changed)
                            print("update")

                        elif TACTICAL_FIGURE_STATE["cached_data"][i] != tactical_figure_datas[i, 1] and \
                            tactical_figure_datas[i, 0] not in TACTICAL_FIGURE_STATE["removed_data"] and \
                            tactical_figure_datas[i, 1] == "REMOVE":
                            TACTICAL_FIGURE_STATE["removed_data"].extend(tactical_figure_datas[i, 1])
                            del TACTICAL_FIGURE_STATE["cached_data"][i]
                            changed_data.append(data_has_changed)
                            print("deleted")

                print("changed data", changed_data)
                if len(changed_data) > 0:
                    datas_changed += 1
                    TACTICAL_FIGURE_STATE["cached_data"] = list(shiptrack_data[0:, 1])
                    if USERS:
                        message = json.dumps(changed_data, default=str)
                        await asyncio.wait([user.send(message) for user in USERS])

        # reference points ------------------------------------------------------------------------
        reference_point_datas = np.array(reference_point_data())

        if len(reference_point_datas) > 0: 
            check_object_id = np.setdiff1d(reference_point_datas[0:, 0], np.array(REFERENCE_POINT_STATE["existed_data"]))
            if len(check_object_id) > 0:
                REFERENCE_POINT_STATE["existed_data"].extend(check_object_id)
                REFERENCE_POINT_STATE["cached_data"] = list(reference_point_datas[0:, 1])
                await send_cached_data()

            if len(check_object_id) == 0:
                changed_data = []
                if len(REFERENCE_POINT_STATE["cached_data"]) > 0:
                    for i, data_has_changed in enumerate(reference_point_datas[0:, 1]):
                        if REFERENCE_POINT_STATE["cached_data"][i] != reference_point_datas[i, 1] and \
                            reference_point_datas[i, 0] not in REFERENCE_POINT_STATE["removed_data"]:
                            changed_data.append(data_has_changed)
                            print("update")

                        elif REFERENCE_POINT_STATE["cached_data"][i] != reference_point_datas[i, 1] and \
                            reference_point_datas[i, 0] not in REFERENCE_POINT_STATE["removed_data"] and \
                            reference_point_datas[i, 1] == "REMOVE":
                            REFERENCE_POINT_STATE["removed_data"].extend(reference_point_datas[i, 1])
                            del REFERENCE_POINT_STATE["cached_data"][i]
                            changed_data.append(data_has_changed)
                            print("deleted")

                print("changed data", changed_data)
                if len(changed_data) > 0:
                    datas_changed += 1
                    REFERENCE_POINT_STATE["cached_data"] = list(shiptrack_data[0:, 1])
                    if USERS:
                        message = json.dumps(changed_data, default=str)
                        await asyncio.wait([user.send(message) for user in USERS])

        # area alerts ------------------------------------------------------------------------
        area_alert_datas = np.array(area_alert_data())
        
        if len(area_alert_datas) > 0: 
            check_object_id = np.setdiff1d(area_alert_datas[0:, 0], np.array(AREA_ALERT_STATE["existed_data"]))
            if len(check_object_id) > 0:
                AREA_ALERT_STATE["existed_data"].extend(check_object_id)
                AREA_ALERT_STATE["cached_data"] = list(area_alert_datas[0:, 1])
                await send_cached_data()

            if len(check_object_id) == 0:
                changed_data = []
                if len(AREA_ALERT_STATE["cached_data"]) > 0:
                    for i, data_has_changed in enumerate(area_alert_datas[0:, 1]):
                        if AREA_ALERT_STATE["cached_data"][i] != area_alert_datas[i, 1] and \
                            area_alert_datas[i, 0] not in AREA_ALERT_STATE["removed_data"]:
                            changed_data.append(data_has_changed)
                            print("update")

                        elif AREA_ALERT_STATE["cached_data"][i] != area_alert_datas[i, 1] and \
                            area_alert_datas[i, 0] not in AREA_ALERT_STATE["removed_data"] and \
                            area_alert_datas[i, 1] == "REMOVE":
                            AREA_ALERT_STATE["removed_data"].extend(area_alert_datas[i, 1])
                            del AREA_ALERT_STATE["cached_data"][i]
                            changed_data.append(data_has_changed)
                            print("deleted")

                print("changed data", changed_data)
                if len(changed_data) > 0:
                    datas_changed += 1
                    AREA_ALERT_STATE["cached_data"] = list(shiptrack_data[0:, 1])
                    if USERS:
                        message = json.dumps(changed_data, default=str)
                        await asyncio.wait([user.send(message) for user in USERS])

        if datas_changed > 0:
            await send_cached_data()
        print("its sending data!")

        # lama tidur
        await asyncio.sleep(4)

async def handler(websocket, path):
    await register(websocket),
    try:
        await send_cached_data()
        async for message in websocket:
            pass
    except websockets.exceptions.ConnectionClosedError:
        print("connection error")
    finally:
        await unregister(websocket)

# start_server = websockets.serve(handler, "10.20.112.203", 8080)
start_server = websockets.serve(handler, "172.16.16.12", 8080)

tasks = [
    asyncio.ensure_future(data_change_detection()),
    asyncio.ensure_future(start_server)
]

asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks))
asyncio.get_event_loop().run_forever()
