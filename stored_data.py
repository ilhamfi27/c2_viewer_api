import psycopg2
from functools import reduce
from base.db_connection import getconn
from actions.data import *

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
                        'source','track_name','iu_indicator','airborne_indicator', 'initiation_time')
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
                        "   airborne_indicator," \
                        "   initiation_time " \
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

def replay_data(session_id):

    try:
        columns = (
            'rp.id',
            'session_id',
            'update_rate',
            'name',
            'start_time',
            'end_time',
            'data'
        )

        q = "SELECT rp.id, session_id, update_rate, name, start_time, end_time, data " \
            "FROM stored_replay rp " \
            "JOIN sessions s ON s.id = rp.session_id " \
            "WHERE session_id = {};".format(session_id)
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
        columns = (
            'latitude',
            'longitude',
            'last_update_time',
        )
        the_query = "select " \
                    "   max(latitude), " \
                    "   max(longitude), " \
                    "   last_update_time " \
                    "from public.replay_system_track_kinetic " \
                    "where system_track_number = {} " \
                    "group by last_update_time " \
                    "order by last_update_time asc;" \
                    .format(system_track_number)
        cur.execute(the_query)
        data = []
        for row in cur.fetchall():
            results = dict(zip(columns, row))
            data.append(results)
        return data
    except psycopg2.Error as e:
        print(e)
    cur.close()
    conn.close()
