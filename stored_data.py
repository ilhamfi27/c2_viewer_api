import random
import psycopg2
import numpy as np
import time
import datetime
import asyncio
import json
from functools import reduce
from connection import getconn 

conn = getconn()

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

async def send_message(USERS, message_data, data_category):
    if USERS:
        send_data = dict()
        send_data[data_category] = message_data
        message = json.dumps({'data': send_data, 'data_type': 'realtime'}, default=str)
        await asyncio.wait([user.send(message) for user in USERS])

async def send_notification(USERS, message_data, data_category):
    if USERS:
        send_data = dict()
        send_data[data_category] = message_data
        message = json.dumps({'data': send_data, 'data_type': 'notification'}, default=str)
        await asyncio.wait([user.send(message) for user in USERS])

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
            columns = ('system_track_number','created_time','identity','environment',
                        'source','track_name','iu_indicator','airborne_indicator')
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
                        "   iu_indicator, " \
                        "   airborne_indicator " \
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
                        "   heading," \
                        "   latitude," \
                        "   longitude," \
                        "   height_depth," \
                        "   speed_over_ground," \
                        "   course_over_ground, " \
                        "   last_update_time " \
                        "FROM " + ar_mandatory_table_8[ix] + " " \
                        "WHERE session_id = " + str(session_id) + " " \
                        "AND system_track_number = " + str(ready) + " " \
                        "ORDER BY created_time DESC) aa LIMIT 1;"
                    cur.execute(q)
                    for row in cur.fetchall():
                        results['heading'] = row[0]
                        results['latitude'] = row[1]
                        results['longitude'] = row[2]
                        results['height_depth'] = row[3]
                        results['speed_over_ground'] = row[4]
                        results['course_over_ground'] = row[5]
                        results['last_update_time'] = row[6]

                if(ar_mandatory_table_8[ix]=='replay_system_track_processing'):
                    q = "SELECT * FROM" \
                        "( SELECT " \
                        "   track_join_status," \
                        "   track_fusion_status," \
                        "   track_phase_type, " \
                        "   track_suspect_level, " \
                        "   created_time " \
                        "FROM " + ar_mandatory_table_8[ix] + " " \
                        "WHERE session_id = " + str(session_id) + " " \
                        "AND system_track_number = " + str(ready) + " " \
                        "ORDER BY created_time DESC) aa LIMIT 1;"
                    cur.execute(q)
                    for row in cur.fetchall():
                        if len(results) > 0:
                            results['track_join_status'] = row[0]
                            results['track_fusion_status'] = row[1]
                            results['track_phase_type'] = row[2]
                            results['track_suspect_level'] = row[3]
                            results['created_time'] = row[4]
                
                if(ar_mandatory_table_8[ix]=='replay_system_track_link'):
                    q = "SELECT * FROM" \
                        "( SELECT " \
                        "   network_track_number," \
                        "   originator_address," \
                        "   link_status " \
                        "FROM " + ar_mandatory_table_8[ix] + " " \
                        "WHERE session_id = " + str(session_id) + " " \
                        "AND system_track_number = " + str(ready) + " " \
                        "ORDER BY created_time DESC) aa LIMIT 1;"
                    cur.execute(q)
                    for row in cur.fetchall():
                        if len(results) > 0:
                            results['network_track_number'] = row[0]
                            results['originator_address'] = row[1]
                            results['link_status'] = row[2]
                
                if(ar_mandatory_table_8[ix]=='replay_system_track_mission'):
                    q = "SELECT * FROM" \
                        "( SELECT " \
                        "   mission_name," \
                        "   route," \
                        "   voice_call_sign, " \
                        "   voice_frequency_channel, " \
                        "   fuel_status, " \
                        "   start_time, " \
                        "   end_time " \
                        "FROM " + ar_mandatory_table_8[ix] + " " \
                        "WHERE session_id = " + str(session_id) + " " \
                        "AND system_track_number = " + str(ready) + " " \
                        "ORDER BY created_time DESC) aa LIMIT 1;"
                    cur.execute(q)
                    for row in cur.fetchall():
                        if len(results) > 0:
                            results['mission_name'] = row[0]
                            results['route'] = row[1]
                            results['voice_call_sign'] = row[2]
                            results['voice_frequency_channel'] = row[3]
                            results['fuel_status'] = row[4]
                            results['start_time'] = row[5]
                            results['end_time'] = row[6]
                            results['link_status'] = row[2]
                
                if(ar_mandatory_table_8[ix]=='replay_system_track_identification'):
                    q = "SELECT * FROM" \
                        "( SELECT " \
                        "   air_platform," \
                        "   surf_platform," \
                        "   land_platform, " \
                        "   air_platform_activity, " \
                        "   surf_platform_activity, " \
                        "   land_platform_activity, " \
                        "   air_specific, " \
                        "   surf_specific, " \
                        "   land_specific " \
                        "FROM " + ar_mandatory_table_8[ix] + " " \
                        "WHERE session_id = " + str(session_id) + " " \
                        "AND system_track_number = " + str(ready) + " " \
                        "ORDER BY created_time DESC) aa LIMIT 1;"
                    cur.execute(q)
                    for row in cur.fetchall():
                        if len(results) > 0:
                            results['air_platform'] = row[0]
                            results['surf_platform'] = row[1]
                            results['land_platform'] = row[2]
                            results['air_platform_activity'] = row[3]
                            results['surf_platform_activity'] = row[4]
                            results['land_platform_activity'] = row[5]
                            results['air_specific'] = row[6]
                            results['surf_specific'] = row[7]
                            results['land_specific'] = row[8]

                if(ar_mandatory_table_8[ix]=='replay_ais_data'):
                    if(source_data=='AIS_TYPE'):
                        data_ais = 0
                        q = "SELECT " \
                            "   * " \
                            "FROM " \
                            "(" \
                            "   SELECT " \
                            "       mmsi_number, " \
                            "       name as ship_name, " \
                            "       radio_call_sign, " \
                            "       imo_number, " \
                            "       navigation_status, " \
                            "       destination, " \
                            "       dimensions_of_ship, " \
                            "       type_of_ship_or_cargo, " \
                            "       rate_of_turn, " \
                            "       gross_tonnage, " \
                            "       ship_country, " \
                            "       eta_at_destination, " \
                            "       vendor_id " \
                            "   FROM " + ar_mandatory_table_8[ix] +" " \
                            "   WHERE session_id = " + str(session_id) +" " \
                            "   AND system_track_number = " + str(ready) +" " \
                            "   ORDER BY created_time DESC " \
                            ") aa LIMIT 1;"
                        cur.execute(q)
                        for row in cur.fetchall():
                            if len(results) > 0:
                                data_ais += 1
                                results['mmsi_number'] = row[0]
                                results['ship_name'] = row[1]
                                results['radio_call_sign'] = row[2]
                                results['imo_number'] = row[3]
                                results['navigation_status'] = row[4]
                                results['destination'] = row[5]
                                results['dimensions_of_ship'] = row[6]
                                results['type_of_ship_or_cargo'] = row[7]
                                results['rate_of_turn'] = row[8]
                                results['gross_tonnage'] = row[9]
                                results['ship_country'] = row[10]
                                results['eta_at_destination'] = row[11]
                                results['vendor_id'] = row[12]
                        if data_ais > 0:
                            ship_tracks.append([system_track_number, results])
                            continue
                    else:
                        if len(results) > 0:
                            results['type_of_ship_or_cargo'] = '-'
                            results['ship_name'] = '-'
                        ship_tracks.append([system_track_number, results])
                # pengambilan data track diambil dari pengkondisian ais
                # tidak di kolektif di bawah
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
            object_id = row[1]
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

        q = "SELECT DISTINCT aa.* " \
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
            object_id = row[2]
            results = dict(zip(columns, row))
            data.append([object_id, results])
        return data
    except psycopg2.Error as e:
        print(e)
    cur.close()
    conn.close()

def session_data():
    try:
        columns = (
            'id','name','start_time','end_time'
        )

        q = "SELECT id, name, start_time, end_time " \
            "FROM public.sessions " \
            "WHERE end_time IS NOT NULL;"
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

async def data_processing(important_data, STATE, USERS, NON_REALTIME_USERS, 
                             data_category='', mandatory_attr='', must_remove=[], debug=True):
    # cek apakah datanya berisi?
    if len(important_data) > 0: 
        # variable existing data digunakan untuk mengecek apakah datanya baru atau data lama
        existing_data = STATE['existed_data'] + STATE['removed_data']

        if debug:
            print(data_category + ' existing ', existing_data)
            print(data_category + ' = clean existing data', STATE['existed_data'])
            print(data_category + ' = removed data', STATE['removed_data'])

        # mengambil data selisih dari variable yang sudah ada dan data yang baru
        check_track_number_system = np.setdiff1d(important_data[0:, 0], np.array(existing_data))

        if debug:
            print(data_category + ' new track length', len(check_track_number_system))
            print(data_category + ' new track_number_system', check_track_number_system)

        # cek apakah datanya lebih dari 0
        if len(check_track_number_system) > 0:

            # jika masuk ke kondisi ini, maka ada data yang baru dari database
            new_datas = []
            for i, new_data in enumerate(important_data):

                # jika data yang diterima statusnya deleted, maka akan dikondisikan ke variable delete
                if new_data[1][mandatory_attr] in must_remove and \
                    new_data[0] not in STATE['removed_data']:
                    STATE['removed_data'].append(new_data[0])
                else:
                    # jika data tidak di memori existed data dan removed data
                    # maka akan membuat data baru dan dikirim ke user 
                    if new_data[0] not in STATE['existed_data'] and \
                        new_data[0] not in STATE['removed_data']:
                        STATE['existed_data'].append(new_data[0])

                        # struktur cached data adalah array of 2d arrays
                        STATE['cached_data'].append(new_data)
                        new_datas.append(new_data[1])
            await send_message(USERS, new_datas ,data_category)
            await send_notification(NON_REALTIME_USERS, new_datas ,data_category)

        # jika data tidak ada perubahan jumlah
        if len(check_track_number_system) == 0:
            changed_data = []

            # mengecek apakah datanya terdapat pada memori remove ?
            # jika iya maka data akan di proses dan dikirim sebagai data baru 
            for i, data in enumerate(important_data):
                if data[0] in STATE['removed_data'] and \
                    important_data[i, 1][mandatory_attr] not in must_remove:
                    STATE['existed_data'].append(data[0])
                    STATE['cached_data'].append(data)
                    STATE['removed_data'].remove(data[0])
                    changed_data.append(data)
                    if debug:
                        print(data_category + ' brand new')

            if len(STATE['cached_data']) > 0:
                for i, data in enumerate(STATE['cached_data']):

                    # cari data dari important data dimana id nya sama dengan id yang ada
                    # di cached data
                    important_data_idx = np.where(important_data[:, 0] == data[0])

                    # kalau datanya ada
                    if len(important_data_idx[0]) > 0:

                        # jika data tidak sama dengan data yang baru, 
                        # int(important_data_idx[0]) isinya 1 angka hasil dari pencarian index di atas
                        if data[1] != important_data[int(important_data_idx[0]), 1]:

                            # maka akan di proses dengan pengecekan apakah statusnya delete atau bukan
                            if important_data[int(important_data_idx[0]), 1][mandatory_attr] in must_remove:

                                # status delete akan mengganti data dalam memori removed data dan existed data dan 
                                # menghapus data dari memori existed
                                STATE['removed_data'].append(data[0])
                                STATE['existed_data'].remove(data[0])
                                STATE['cached_data'].pop(i)

                                # data akan dikirim ke user
                                changed_data.append(important_data[int(important_data_idx[0]), :])
                                if debug:
                                    print(data_category + ' deleted')
                            else:
                                # jika status data selain deleted maka memori cached data akan direplace dengan data baru
                                # dan dikirim ke user
                                changed_data.append(data)
                                STATE['cached_data'][i][1] = important_data[int(important_data_idx[0]), 1]
                                if debug:
                                    print(data_category + ' updated')
                    else:
                        if debug:
                            print('can\'t find data', data[0])
            if debug:
                print(data_category + ' track changed', changed_data)
            changed = np.array(changed_data)
            if len(changed_data) > 0:
                await send_message(USERS, list(changed[0:, 1]) ,data_category)
                await send_notification(NON_REALTIME_USERS, list(changed[0:, 1]) ,data_category)

    if debug:
        print(data_category + ' \n==', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), '\n\n')

async def non_strict_data_processing(important_data, STATE, USERS, NON_REALTIME_USERS, data_category='', debug=True):
    # cek apakah datanya berisi?
    if len(important_data) > 0: 
        # variable existing data digunakan untuk mengecek apakah datanya baru atau data lama
        existing_data = STATE['existed_data']

        if debug:
            print(data_category + ' existing ', existing_data)

        # mengambil data selisih dari variable yang sudah ada dan data yang baru
        check_track_number_system = np.setdiff1d(important_data[0:, 0], np.array(existing_data))

        if debug:
            print(data_category + ' new track length', len(check_track_number_system))
            print(data_category + ' new track_number_system', check_track_number_system)

        # cek apakah datanya lebih dari 0
        if len(check_track_number_system) > 0:

            # jika masuk ke kondisi ini, maka ada data yang baru dari database
            new_datas = []
            for i, new_data in enumerate(important_data):

                # struktur cached data adalah array of 2d arrays
                if new_data[0] not in STATE['existed_data']:
                    STATE['existed_data'].append(new_data[0])
                    STATE['cached_data'].append(new_data)
                    new_datas.append(new_data[1])
            await send_message(USERS, new_datas ,data_category)
            await send_notification(NON_REALTIME_USERS, new_datas ,data_category)

        # jika data tidak ada perubahan jumlah
        if len(check_track_number_system) == 0:
            changed_data = []

            if len(STATE['cached_data']) > 0:
                for i, data in enumerate(STATE['cached_data']):

                    # cari data dari important data dimana id nya sama dengan id yang ada
                    # di cached data
                    important_data_idx = np.where(important_data[:, 0] == data[0])

                    # kalau datanya ada
                    if len(important_data_idx[0]) > 0:

                        # jika data tidak sama dengan data yang baru, 
                        # int(important_data_idx[0]) isinya 1 angka hasil dari pencarian index di atas
                        if data[1] != important_data[int(important_data_idx[0]), 1]:
                            changed_data.append(data)
                            STATE['cached_data'][i][1] = important_data[int(important_data_idx[0]), 1]
                            if debug:
                                print(data_category + ' updated')
                    else:
                        if debug:
                            print('can\'t find data', data[0])
            if debug:
                print(data_category + ' track changed', changed_data)
            changed = np.array(changed_data)
            if len(changed_data) > 0:
                await send_message(USERS, list(changed[0:, 1]) ,data_category)
                await send_notification(NON_REALTIME_USERS, list(changed[0:, 1]) ,data_category)

    if debug:
        print(data_category + ' \n==', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), '\n\n')
