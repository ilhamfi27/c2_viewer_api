import json
import psycopg2
import numpy as np
from functools import reduce
from datetime import datetime
import time

import tracker.util as util
from tracker.config import getconn, r
from tracker.state import *

conn = getconn()
cur = conn.cursor()

r.flushdb()  # flushing db

now = datetime.now()

ar_mandatory_table = [
    'replay_system_track_general',
    'replay_system_track_kinetic',
    'replay_system_track_processing',
]
ar_mandatory_table_8 = [
    'replay_system_track_general',
    'replay_system_track_kinetic',
    'replay_system_track_processing',
    'replay_system_track_identification',
    'replay_system_track_link',
    'replay_system_track_mission',
    'replay_track_general_setting',
    'replay_ais_data',
]


# ============ improvements ============
# model action
def data_process(table, stn, table_results):
    """
    method ini dipakai buat set data ke redis
    dan mengecek untuk pengiriman data ke user
    pengecekan:
    1. kelengkapan data mandatory
        - kirim data ketika lengkap
        - simpan data ketika belum lengkap
    2. eksistensi data di memory
        - jika ada, dan data beda, maka kirim data yang baru dan update memory
        - jika ada, data beda, dan statusnya removed, maka kirim status removed, dan hapus dari memory

    must read:
    di redis disimpen 2 list data
    - hashed -> . memori untuk menyimpan semua data track. Bakal dipanggil waktu ada user baru yang connect
                    jadi langsung ngasih semua data yang di memori ini
                . di redis disimpen dalam hash dengan nama 'tracks'
    - pair key value -> . memori untuk menyimpan data track yang 'available' atau 'not removed by system'
                        . dipakai buat ngecek data apakah data itu sudah pernah terhapus atau belum ***
                        . nama key -> T + STN = 'T2' / 'T3' / 'T4'
                        . data yang disimpen di memori ini adalah data lengkap dan tidak dihapus
    """
    if table_results == {}:
        return None, None
    util.datetime_to_string(table_results)  # convert datetime to string
    # util.string_bool_to_bool(table_results)  # clean STRING BOOLEAN to PURE BOOLEAN

    is_updated = False
    is_newly_created = False

    track_index = 'T' + str(stn)

    if r.hexists('tracks', stn):  # kalau data dari redis udh ada
        stn_hash = json.loads(r.hget('tracks', stn).decode("utf-8"))  # get data dari redis + convert to dict
        stn_hash.pop('completed', None)  # remove created key dari hash redis

        # table -> table name dari looping
        if table in stn_hash:  # kalau tablenya udh ada dari redis
            if stn_hash[table] != table_results:  # kalau hash dari redis gak sama dengan hasil dari query
                stn_hash[table] = table_results
                is_updated = True
        else:  # kalau tablenya gak ada dari redis tapi datanya udah ada
            stn_hash[table] = table_results  # set hash dengan nama table
            is_updated = True

        # double check dengan list STN dari memory
        # *** cek apakah di memory pernah dihapus atau enggak
        if table == 'replay_system_track_processing' \
                and table_results['track_phase_type'] not in ["DELETED_BY_SYSTEM", "DELETED_BY_SENSOR"] \
                and not r.exists(track_index) and is_updated:
            is_updated = False;
            is_newly_created = True

    else:  # kalau data dari redis kosong
        stn_hash = {}
        stn_hash[table] = table_results  # set hash dengan nama table
        is_newly_created = True

    # cek kelengkapan di 3 tabel
    three_mandatory_completed = stn_hash.keys() >= {
        'replay_system_track_general',
        'replay_system_track_kinetic',
        'replay_system_track_processing'
    }

    # kasih flag kelengkapan data
    if three_mandatory_completed:
        if stn_hash['replay_system_track_general']['source'] == 'AIS_TYPE':
            stn_hash['completed'] = True if "replay_ais_data" in list(stn_hash.keys()) else False
        else:
            stn_hash['completed'] = True
    else:
        stn_hash['completed'] = False

    r.hset('tracks', stn, json.dumps(stn_hash))  # set redis key dengan STN

    # jika lengkap, maka data dikirimkan ke user
    if stn_hash['completed']:
        if stn_hash['replay_system_track_processing']['track_phase_type'] not in ["DELETED_BY_SYSTEM",
                                                                                  "DELETED_BY_SENSOR"]:
            r.set(track_index, json.dumps(stn_hash))  # set data track ke redis

            if is_newly_created:  # kalau datanya dideteksi baru
                return 'new', stn_hash

        if r.exists(track_index) and is_updated:  # kalau datanya dideteksi update, dan di redis ada

            # kalau tracknya dihapus, maka hapus dari memory
            if stn_hash['replay_system_track_processing']['track_phase_type'] in \
                    ["DELETED_BY_SYSTEM", "DELETED_BY_SENSOR"]:

                if r.exists(track_index): r.delete(track_index)

            return 'update', table_results

    return None, None


async def improved_track_data():
    # mengambil waktu sekarang untuk dibandingkan di query
    current_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    try:
        start_time_session = "SELECT " \
                             "   start_time " \
                             "FROM sessions " \
                             "WHERE end_time IS NULL;"
        cur.execute(start_time_session)
        start_time = cur.fetchall()

        # untuk menyimpan create untuk masing - masing 8
        # tabel
        created_time_tracks = {
            "replay_system_track_general": start_time[0][0],
            "replay_system_track_kinetic": start_time[0][0],
            "replay_system_track_processing": start_time[0][0],
            "replay_system_track_identification": start_time[0][0],
            "replay_system_track_link": start_time[0][0],
            "replay_system_track_mission": start_time[0][0],
            "replay_track_general_setting": start_time[0][0],
            "replay_ais_data": start_time[0][0],
        }

        table_columns = {
            "replay_system_track_general": (
                'system_track_number', 'created_time', 'identity', 'environment', 'source', 'track_name',
                'iu_indicator', 'airborne_indicator',
                'initiation_time',
            ),
            "replay_system_track_kinetic": (
                'system_track_number', 'created_time', 'heading', 'latitude', 'longitude', 'height_depth',
                'speed_over_ground', 'course_over_ground', 'last_update_time',
            ),
            "replay_system_track_processing": (
                'system_track_number', 'created_time', 'track_join_status', 'track_fusion_status', 'track_phase_type',
                'track_suspect_level',
            ),
            "replay_system_track_identification": (
                'system_track_number', 'created_time', 'air_platform', 'surf_platform', 'land_platform',
                'air_platform_activity', 'surf_platform_activity', 'land_platform_activity', 'air_specific',
                'surf_specific', 'land_specific',
            ),
            "replay_system_track_link": (
                'system_track_number', 'created_time', 'network_track_number', 'originator_address', 'link_status',
            ),
            "replay_system_track_mission": (
                'system_track_number', 'created_time', 'mission_name', 'route', 'voice_call_sign',
                'voice_frequency_channel', 'fuel_status', 'start_time', 'end_time',
            ),
            "replay_track_general_setting": (
                'system_track_number', 'created_time', 'session_id', 'speed_label_visibility',
                'track_name_label_visibility', 'radar_coverage_visibility', 'track_visibility', 'created_time',
            ),
            "replay_ais_data": (
                'system_track_number', 'created_time', 'mmsi_number', 'name', 'radio_call_sign', 'imo_number',
                'navigation_status', 'destination', 'dimensions_of_ship', 'type_of_ship_or_cargo', 'rate_of_turn',
                'gross_tonnage', 'ship_country',
                'eta_at_destination', 'vendor_id', 'created_time',
            ),
        }

        data_updates = {}
        for ix, table in enumerate(ar_mandatory_table_8):
            column_used = ("st." + s for s in table_columns[table])
            column_used = ", ".join(column_used)
            where_own_indicator = "and st.own_unit_indicator='FALSE'" \
                if table == "replay_system_track_general" else ""
            replay_query = """
                SELECT {}
                FROM {} st
                JOIN sessions s ON st.session_id=s.id
                JOIN (
                        SELECT
                            session_id,
                            system_track_number,
                            max(created_time) created_time
                        FROM {}
                        WHERE created_time >= '{}'
                        AND created_time < '{}'
                        GROUP BY session_id,system_track_number
                    ) mx
                ON st.system_track_number=mx.system_track_number
                and st.created_time=mx.created_time
                and st.session_id=mx.session_id
                WHERE s.end_time is NULL
                {}
                ORDER BY st.system_track_number;
                """.format(column_used, table, table, created_time_tracks[table], current_time,
                           where_own_indicator)

            cur.execute(replay_query)
            data = cur.fetchall()

            for row in data:
                stn = row[0]  # stn -> system_track_number
                table_results = dict(zip(table_columns[table], row))  # make the result dictionary
                created_time_tracks[table] = table_results['created_time']

                status, result = data_process(table, stn, table_results)

                if status != None:
                    if status == 'new':
                        data_updates[stn] = result
                        data_updates[stn]['status'] = 'new'
                        data_updates[stn]['system_track_number'] = stn

                    if status == 'update':
                        if stn in data_updates:
                            data_updates[stn][table] = result
                        else:
                            table_update = {}
                            table_update[table] = result
                            data_updates[stn] = table_update
                            data_updates[stn]['status'] = 'update'
                            data_updates[stn]['system_track_number'] = stn

        updated_data = [val for key, val in data_updates.items()]

        if updated_data != []:
            # kirim ke realtime user
            message = json.dumps({'data': {'tracks': updated_data}, 'data_type': 'realtime'})
            for user in USERS: await user.send(message)

            # kirim ke non realtime user sebagai notifikasi
            notification = json.dumps({'data': {'tracks': updated_data}, 'data_type': 'notification'})
            for user in NON_REALTIME_USERS: await user.send(notification)

    except psycopg2.Error as e:
        print(e)


# ============ improvements ============


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
            columns = ('system_track_number', 'created_time', 'identity', 'environment',
                       'source', 'track_name', 'iu_indicator', 'airborne_indicator', 'initiation_time')
            results = {}
            for ix in range(len(ar_mandatory_table_8)):
                # dapatkan created time yang terakhir per 8 tabel tersebut
                q = "SELECT " \
                    "   max(created_time) created_time " \
                    "FROM " + ar_mandatory_table_8[ix] + " " \
                                                         "WHERE session_id = " + str(session_id) + " " \
                                                                                                   "AND system_track_number = " + str(
                    ready) + ";"
                cur.execute(q)
                data = cur.fetchall()
                for row in data:
                    created_time = str(row[0])

                if (created_time > str(last_system_track_number_kirim_datetime[ix])):
                    # kirimkan data dengan created time terbaru
                    # dan simpan ke last_system_track_number_kirim
                    last_system_track_number_kirim_datetime[ix] = created_time
                if (ar_mandatory_table_8[ix] == 'replay_system_track_general'):
                    q = "SELECT * FROM " \
                        "(  SELECT " \
                        "   system_track_number," \
                        "   created_time,identity," \
                        "   environment," \
                        "   source," \
                        "   track_name," \
                        "   iu_indicator, " \
                        "   airborne_indicator," \
                        "   initiation_time " \
                        "FROM   " + ar_mandatory_table_8[ix] + " " \
                                                               "WHERE session_id = " + str(session_id) + " " \
                                                                                                         "AND system_track_number = " + str(
                        ready) + " ORDER BY created_time DESC) ss LIMIT 1"
                    cur.execute(q)
                    for row in cur.fetchall():
                        system_track_number = row[0]
                        results = dict(zip(columns, row))
                        source_data = row[4]
                if (ar_mandatory_table_8[ix] == 'replay_system_track_kinetic'):
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
                                                                                                       "AND system_track_number = " + str(
                        ready) + " " \
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

                if (ar_mandatory_table_8[ix] == 'replay_system_track_processing'):
                    q = "SELECT * FROM" \
                        "( SELECT " \
                        "   track_join_status," \
                        "   track_fusion_status," \
                        "   track_phase_type, " \
                        "   track_suspect_level, " \
                        "   created_time " \
                        "FROM " + ar_mandatory_table_8[ix] + " " \
                                                             "WHERE session_id = " + str(session_id) + " " \
                                                                                                       "AND system_track_number = " + str(
                        ready) + " " \
                                 "ORDER BY created_time DESC) aa LIMIT 1;"
                    cur.execute(q)
                    for row in cur.fetchall():
                        if len(results) > 0:
                            results['track_join_status'] = row[0]
                            results['track_fusion_status'] = row[1]
                            results['track_phase_type'] = row[2]
                            results['track_suspect_level'] = row[3]
                            results['created_time'] = row[4]

                if (ar_mandatory_table_8[ix] == 'replay_system_track_link'):
                    q = "SELECT * FROM" \
                        "( SELECT " \
                        "   network_track_number," \
                        "   originator_address," \
                        "   link_status " \
                        "FROM " + ar_mandatory_table_8[ix] + " " \
                                                             "WHERE session_id = " + str(session_id) + " " \
                                                                                                       "AND system_track_number = " + str(
                        ready) + " " \
                                 "ORDER BY created_time DESC) aa LIMIT 1;"
                    cur.execute(q)
                    for row in cur.fetchall():
                        if len(results) > 0:
                            results['network_track_number'] = row[0]
                            results['originator_address'] = row[1]
                            results['link_status'] = row[2]

                if (ar_mandatory_table_8[ix] == 'replay_system_track_mission'):
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
                                                                                                       "AND system_track_number = " + str(
                        ready) + " " \
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

                if (ar_mandatory_table_8[ix] == 'replay_system_track_identification'):
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
                                                                                                       "AND system_track_number = " + str(
                        ready) + " " \
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

                if (ar_mandatory_table_8[ix] == 'replay_ais_data'):
                    if (source_data == 'AIS_TYPE'):
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
                            "   FROM " + ar_mandatory_table_8[ix] + " " \
                                                                    "   WHERE session_id = " + str(session_id) + " " \
                                                                                                                 "   AND system_track_number = " + str(
                            ready) + " " \
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
                            results['history_dots'] = history_dots(system_track_number)
                            ship_tracks.append([system_track_number, results])
                            continue
                    else:
                        if len(results) > 0:
                            results['type_of_ship_or_cargo'] = '-'
                            results['ship_name'] = '-'
                        results['history_dots'] = history_dots(system_track_number)
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
            'id', 'name', 'start_time', 'end_time'
        )

        q = "SELECT " \
            "   id, " \
            "   name, " \
            "   start_time, " \
            "   end_time " \
            "FROM public.sessions s " \
            "JOIN ( " \
            "	select session_id " \
            "	from public.stored_replay " \
            "	group by 1 " \
            ") sr on sr.session_id = s.id " \
            "WHERE end_time IS NOT NULL " \
            "ORDER BY s.id; "
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


def history_dots(system_track_number):
    try:
        the_query = "select " \
                    "   max(latitude), " \
                    "   max(longitude), " \
                    "   last_update_time " \
                    "from public.replay_system_track_kinetic k " \
                    "JOIN ( " \
                    "	select id " \
                    "	from sessions " \
                    "	where end_time is null " \
                    ") s on s.id = k.session_id " \
                    "where system_track_number = {} " \
                    "group by last_update_time " \
                    "order by last_update_time asc; " \
            .format(system_track_number)
        cur.execute(the_query)
        data = []
        for row in cur.fetchall():
            results = dict()
            results['latitude'] = row[0]
            results['longitude'] = row[1]
            results['latlng'] = [row[0], row[1]]
            results['last_update_time'] = row[2]
            data.append(results)
        return data
    except psycopg2.Error as e:
        print(e)
    cur.close()
    conn.close()


def all_history_dots(system_track_numbers=[]):
    if system_track_numbers == []:
        return []
    try:
        track_dots = []
        for track in system_track_numbers:
            the_query = "select " \
                        "   max(latitude), " \
                        "   max(longitude), " \
                        "   last_update_time " \
                        "from public.replay_system_track_kinetic k " \
                        "JOIN ( " \
                        "	select id " \
                        "	from sessions " \
                        "	where end_time is null " \
                        ") s on s.id = k.session_id " \
                        "where system_track_number = {} " \
                        "group by last_update_time " \
                        "order by last_update_time asc; " \
                .format(track)
            cur.execute(the_query)
            data = []
            for row in cur.fetchall():
                results = dict()
                results['latitude'] = row[0]
                results['longitude'] = row[1]
                results['latlng'] = [row[0], row[1]]
                results['last_update_time'] = row[2]
                data.append(results)
            track_dots.append({track: data})
        return track_dots
    except psycopg2.Error as e:
        print(e)
    cur.close()
    conn.close()
