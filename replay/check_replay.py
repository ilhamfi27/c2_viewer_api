#!/usr/bin/env python

# WS server example that synchronizes REALTIME_state across clients


from main import *


replay_data_send = []

def replay_track(session_id, start_time, end_time, added_track, data_lengkap):
    # print(start_time, end_time, added_track)
    return_data = []
    track_data = []
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
    ar_mandatory_table = [
        'replay_system_track_general',
        'replay_system_track_kinetic',
        'replay_system_track_processing'
    ]

    # data_lengkap = [[], [], []]
    # BUTUH PERBAIKAN
    i = 0
    for table in ar_mandatory_table:
        column = "st.system_track_number, st.track_phase_type" if table == 'replay_system_track_processing' else "st.system_track_number"
        sql_mandatory = "SELECT "+ column +" \
                        FROM " + table + " st \
                        JOIN( \
                            SELECT system_track_number,max(created_time) created_time \
                            FROM " + table + " \
                            WHERE session_id = " + str(session_id) + " AND created_time > '" + str(
            start_time) + "' AND created_time < '" + str(end_time) + "' \
                            GROUP BY system_track_number \
                        ) mx ON st.system_track_number=mx.system_track_number and st.created_time=mx.created_time \
                        WHERE st.session_id = " + str(session_id) + " AND st.created_time > '" + str(
            start_time) + "' AND st.created_time < '" + str(end_time) + "' \
                        ORDER BY st.system_track_number"
        # print(sql_mandatory)
        cur.execute(sql_mandatory)
        data = cur.fetchall()

        if len(data) > 0:
            for d in data:
                if d[0] not in data_lengkap[i]:
                    data_lengkap[i].append(d[0])    
        i = i +1

    data_ready = reduce(np.intersect1d, data_lengkap)
    # print(data_ready, start_time, end_time)
        # print(data_ready)
    track_final = []
    if len(data_ready) > 0:
        
        for ready in data_ready:
            track = {}
            recorded_track  = {}
            is_exist = False
            for table in ar_mandatory_table_8:
                column = "st.system_track_number, st.iu_indicator" if table == 'replay_system_track_general' else "st.system_track_number"
                check_exist = "SELECT "+column+ " \
                            FROM " + table + " st \
                            WHERE st.session_id = " + str(session_id) + " AND st.system_track_number = "+str(ready)+" AND st.created_time > '" + str(
                start_time) + "' AND st.created_time < '" + str(end_time) + "' \
                            ORDER BY st.system_track_number"
                cur.execute(check_exist)
                data = cur.fetchall()
                
                if table == 'replay_system_track_general':
                    if len(data) > 0:
                        for d in data:
                            if d[1]:
                                check_mission = "SELECT * \
                                    FROM " + table + " st \
                                    WHERE st.session_id = " + str(session_id) + " AND st.system_track_number = "+str(ready)+" AND st.created_time > '" + str(
                        start_time) + "' AND st.created_time < '" + str(end_time) + "' \
                                    ORDER BY st.system_track_number"
                                cur.execute(check_mission)
                                mission_data = cur.fetchall()
                                if len(mission_data):
                                    is_exist = True
                                    break
                    else:
                        if len(data) > 0:
                            is_exist = True
                            break
            
            if is_exist:
                if ready not in track_final:
                    track = {            
                     "system_track_number" : str(ready),
                     "track_name" : "",
                     "track_status" : "",
                     # :eneral,
                     "environment" : "",
                     "source_data" : "",
                     "iu_indicator" : "",
                     "identity" : "",
                     "fusion_status" : "",
                     "join_status" : "",
                     "track_phase" : "",
                     "suspect_level" : "",
                     "initiation_time" : "",
                     "airbone_indicator" : "",
                     # :inetic,
                     "heading" : "",
                     "latitude" : "",
                     "longitude" : "",
                     "altitude" : "",
                     "speed_over_ground" : "",
                     "course_over_ground" : "",
                     "last_update_time" : "",
                     # :is Data,
                     "mmsi_number" : "",
                     "ship_name" : "",
                     "radio_call_sign" : "",
                     "imo_number" : "",
                     "navigation_status" : "",
                     "destination" : "",
                     "dimension_of_ship" : "",
                     "ship_type" : "",
                     "rate_of_turn" : "",
                     "gross_tonnage" : "",
                     "country" : "",
                     "eta" : "",
                     "vendor_id" : "",
                     # :rack link,
                     "network_track_number" : "",
                     "originator" : "",
                     "link_status" : "",
                     # :ission,
                     "mission_name" : "",
                     "mission_route" : "",
                     "voice_call_sig" : "",
                     "voice_frequency_channel" : "",
                     "fuel_status" : "",
                     "mission_start" : "",
                     "mission_finish" : "",
                     # :dentification,
                     "platform_type" : "",
                     "platform_activity" : "",
                     "specific_type" : "",
                     "sql_track" : "",}
                for table in ar_mandatory_table_8:
                    sql_track = "SELECT * FROM " + table + " st \
                                            JOIN (" \
                                                "SELECT system_track_number,max(created_time) created_time " \
                                                "FROM " + table + " " \
                                                "WHERE session_id = '" + str(session_id) + "' \
                                                    AND created_time > '" + start_time + "' AND created_time < '" + end_time + "' \
                                                    GROUP BY system_track_number \
                                            ) mx ON st.system_track_number = mx.system_track_number and st.created_time = mx.created_time \
                                            WHERE st.session_id = " + str(session_id) + " " \
                                                    "AND st.created_time > '" + start_time + "' AND st.created_time < '" + end_time + "' \
                                                    AND st.system_track_number = " + str(ready) + " \
                                                    ORDER BY st.system_track_number"
                    
                    cur.execute(sql_track)
                    data = cur.fetchall()
                    for d in data:
                        # print(cur.description[0])
                        t_status = "T" + str(ready)
                        
                        if table == 'replay_system_track_general':
                            # print(sql_track)
                            track['created_time']        = str(d[1])
                            track['source_data']         = str(d[10])
                            track['track_name']         = str(d[2])
                            track['environment']         = str(d[5])
                            track['iu_indicator']        = str(d[12])
                            track['identity']            = str(d[4])
                            track['initiation_time']     = str(d[19])
                            track['airbone_indicator']     = str(d[21])
                            
                            if (str(d[2]) == 'AIS_TYPE' or str(d[2]) == 'DATALINK'):
                                q_ais_data = "SELECT  * \
                                            FROM  \
                                            ( \
                                            SELECT * \
                                                FROM replay_ais_data  \
                                            WHERE session_id = " + str(session_id) + "   \
                                            AND system_track_number = " + str(ready) + "  \
                                                AND created_time > '" + start_time + "'  \
                                                AND created_time < '" + end_time + "'  \
                                            ORDER BY created_time DESC  \
                                            ) aa LIMIT 1;"
                                cur.execute(q_ais_data)
                                data = cur.fetchall()
                                if len(data) > 0:
                                    track['mmsi_number']         = str(d[2])
                                    track['ship_name']       = str(d[3])
                                    track['radio_call_sign'] = str(d[4])
                                    track['imo_number'] = str(d[5])
                                    track['navigation_status'] = str(d[6])
                                    track['destination'] = str(d[7])
                                    track['dimension_of_ship'] = str(d[8])
                                    track['ship_type'] = str(d[9])
                                    track['rate_of_turn'] = str([10])
                                    track['gross_tonnage'] = str([12])
                                    track['country'] = str([13])
                                    track['eta'] = str([15])
                                    track['vendor_id'] = str([16])

                            if track['iu_indicator']:    
                                sql_mission = "SELECT * FROM replay_system_track_mission st \
                                            JOIN (" \
                                                "SELECT system_track_number,max(created_time) created_time " \
                                                "FROM replay_system_track_mission " \
                                                "WHERE session_id = '" + str(session_id) + "' \
                                                    AND created_time > '" + start_time + "' AND created_time < '" + end_time + "' \
                                                    GROUP BY system_track_number \
                                            ) mx ON st.system_track_number = mx.system_track_number and st.created_time = mx.created_time \
                                            WHERE st.session_id = " + str(session_id) + " " \
                                                    "AND st.created_time > '" + start_time + "' AND st.created_time < '" + end_time + "' \
                                                    AND st.system_track_number = " + str(ready) + " \
                                                    ORDER BY st.system_track_number"
                    
                                cur.execute(sql_mission)
                                data = cur.fetchall()  
                                for d in data:                      
                                    track['mission_name']            = str(d[2])
                                    track['mission_route']           = str(d[3])
                                    track['voice_call_sig']          = str(d[4])
                                    track['voice_frequency_channel'] = str(d[5])
                                    track['fuel_status']             = str(d[6])
                                    track['mission_start']           = str(d[8])
                                    track['mission_finish']          = str(d[9])
                                    track['created_time'] = str(d[10])

                        if table == 'replay_system_track_processing':
                            track['fusion_status']       = str(d[2])
                            track['join_status']         = str(d[3])
                            track['track_phase']         = str(d[5])
                            track['suspect_level']       = str(d[6])
                            track['created_time']        = str(d[7])
                            track_status = "T" + str(ready)
                            if track_status not in added_track:
                                track["track_status"] = track_status+ "A"
                                added_track.append(track_status)
                            else:
                                if track['track_phase'] in ['DELETED_BY_SYSTEM', 'DELETED_BY_SYSTEM']: 
                                    for track_lengkap in data_lengkap: 
                                        if ready in track_lengkap:                                  
                                            track_lengkap.remove(ready)
                                else:
                                    track["track_status"] = track_status + "U"

                        if table == 'replay_system_track_kinetic':
                            track['heading']             = str(d[3])
                            track['latitude']            = str(d[4])
                            track['longitude']           = str(d[5])
                            track['altitude']            = str(d[8])
                            track['speed_over_ground']   = str(d[9])
                            track['course_over_ground']  = str(d[10])
                            track['last_update_time']    = str(d[11])
                            track['created_time'] = str(d[12])
                            
                        if table == 'replay_system_track_link':
                            track['network_track_number']    = str(d[3])
                            track['originator']              = str(d[5])
                            track['link_status']             = str(d[8])
                            track['created_time'] = str(d[9])                
                                            
                        if table == 'replay_system_track_identification':
                            track['environment'] = d[3]
                            if d[3] == 'AIR':
                                track['platform_type']       = str(d[4])
                                track['platform_activity']   = str(d[7])
                                track['specific_type']       = str(d[10])
                            elif d[3] == 'SURFACE':
                                track['platform_type']       = str(d[5])
                                track['platform_activity']   = str(d[8])
                                track['specific_type']       = str(d[11])
                            else:
                                track['platform_type']       = str(d[6])
                                track['platform_activity']   = str(d[9])
                                track['specific_type']       = str(d[12])
                            track['created_time'] = str(d[13])
                        


                        # if t_status not in recorded_track:
                        #     recorded_track[t_status] = str(created_time)
                        # else:
                        #     if created_time > str(recorded_track[t_status]):
                        #         recorded_track[t_status] = str(created_time)
                track_final.append(track)        
    return_data.append(track_final)
    return_data.append(added_track)
    return_data.append(data_lengkap)
    return return_data



# q = "SELECT aa.session_id as id, aa.*  FROM area_alerts aa  JOIN (    SELECT object_id,max(last_update_time) last_update_time     FROM area_alerts     WHERE session_id = '1' AND last_update_time > '2020-01-10 14:14:31' AND last_update_time < '2020-01-10 14:14:41'     GROUP BY object_id ) mx ON aa.object_id=mx.object_id and aa.last_update_time=mx.last_update_time  WHERE aa.session_id = '1'  AND aa.last_update_time > '2020-01-10 14:14:31' AND aa.last_update_time < '2020-01-10 14:14:41'  ORDER BY aa.object_id"

def get_replay():
    '''Get data session yang sudah selesai'''
    sql = "select id, to_char (start_time::timestamp, 'YYYY-MM-DD HH24:MI:SS') start_time, " \
                  " to_char (end_time::timestamp, 'YYYY-MM-DD HH24:MI:SS') end_time, " \
                  "extract(epoch from (end_time::timestamp - start_time::timestamp)) as durasi, name " \
                  " from sessions " \
                  "WHERE end_time IS NOT null"

    cur.execute(sql)
    query = cur.fetchall()
    track = []
    for data in query:
        session_id  = data[0]
        start_time  = data[1]
        end_time    = data[2]
        durasi      = data[3]
        name        = data[4]
        '''Buat panjang durasi dibagi dengan UPDATE_RATE. Buat list sesuai dengan panjang_replay'''
        panjang_replay = durasi / UPDATE_RATE
        track_list = [i for i in range(int(panjang_replay))]
        track_list = dict.fromkeys(track_list, "")
        result={
                        "session_id"        : session_id,
                        "start_time"        : start_time,
                        "end_time"          : end_time,
                        "session_name"      : name,
                        "update_rate"       : UPDATE_RATE,
                        "durasi_session"    : durasi,
                        "track_play"        : track_list
                }
        start_time  = (datetime.strptime(str(start_time), '%Y-%m-%d %H:%M:%S'))
        end_time    = (datetime.strptime(str(end_time), '%Y-%m-%d %H:%M:%S'))
        added_track = []
        '''Looping sebanyak panjang replay'''
        for t in range(len(track_list)+1):
            '''Buat start_time dan end_time untuk setiap segmen replay.
                        Segmen durasi adalah satuan  replay track, 
                        contoh 2020-01-10 14:45:31 sampai dengan 2020-01-10 14:45:41
                        disebut sebagai 1 segmen durasi'''

                    # print(t)
                    # print(str(start_time) + " sampai dengan " + str(end_time))
            if t == 0:
                    tmp_time = (datetime.strptime(str(start_time), '%Y-%m-%d %H:%M:%S'))
                    tmp_time += dt.timedelta(seconds=UPDATE_RATE)
                    end_time = tmp_time
            else:
                    start_time += dt.timedelta(seconds=UPDATE_RATE)
                    end_time += dt.timedelta(seconds=UPDATE_RATE)
            track_data = {
                        "start_segment" : str(start_time),
                        "end_segment"   : str(end_time),
                        "data"          : {
                                "track" : [],
                                "tactical_figures" : [],
                                "reference_point":[],
                                "area_alert" : []

                        }
            }
            '''Jalankan query untuk setiap tabel per setiap segmen durasi'''

            track_replay_data = replay_track(session_id, str(start_time), str(end_time), added_track, data_lengkap)
            result["track_play"][str(t)]["track"].append(track_replay_data[0])            
            added_track = track_replay_data[1]
            data_lengkap = track_replay_data[2]
            query_tf = "SELECT tf.* " \
                                   "FROM tactical_figures tf " \
                                    "JOIN(" \
                                   "     SELECT object_id,max(last_update_time) last_update_time " \
                                   "     FROM tactical_figures " \
                                   "     WHERE session_id = " + str(session_id) + " AND last_update_time > '"+str(start_time)+"' AND last_update_time < '"+str(end_time)+"' " \
                                    "     GROUP BY object_id) mx " \
                                    "ON tf.object_id=mx.object_id and tf.last_update_time=mx.last_update_time " \
                                    "WHERE tf.session_id = '"+str(session_id)+"' AND tf.last_update_time > '"+str(start_time)+"' AND tf.last_update_time < '"+str(end_time)+"' " \
                                    "ORDER BY tf.object_id"
            # print(query_tf)
            cur.execute(query_tf)
            data_tf = cur.fetchall()
            for tf in data_tf:
                object_id                   = tf[2]
                is_visible                  = tf[8]
                tf_name                     = tf[3]
                tf_environment              = tf[4]
                tf_ntn                      = tf[10]
                tf_link_status              = tf[11]
                tf_amplification            = tf[13]
                tf_last_update_time         = tf[15]
                tf_status                   = 'F'+str(object_id)
                if tf_status not in added_track:
                    added_track.append(tf_status)
                    tf_status = tf_status + 'A'
                else:
                    if is_visible == 'REMOVE':
                        added_track.remove(tf_status)
                        tf_status_ = tf_status + 'R'
                    else:
                        tf_status = tf_status + 'U'
                # tf_track = [tf_status, tf_name, tf_environment, tf_ntn, tf_link_status, tf_amplification, str(tf_last_update_time)]
                tf_track = {"system_track_number": tf_status,
                            "object_id": object_id,
                            "object_type": tf[1],
                            "object_id": object_id,
                            "tf_name": tf_name,
                            "tf_environment": tf_environment,
                            "tf_shape": tf[5],
                            "tf_line_color": tf[6],
                            "tf_fill_color": tf[7],
                            "is_visible": is_visible,
                            "last_update_time": str(tf_last_update_time),
                            "network_track_number": tf_ntn,
                            "link_status_type": tf_link_status,
                            "is_editable": tf[12],
                            "point_amplification_type": tf_amplification,
                            "point_keys": tf[14],
                            "points": tf[15]
                            }
                track_data['data']['tactical_figures'].append(tf_track)

            query_rp = "SELECT rrp.* " \
                               "FROM replay_reference_point rrp \
                               JOIN (" \
                               "    SELECT object_id,max(last_update_time) last_update_time " \
                               "    FROM replay_reference_point " \
                               "    WHERE session_id = " + str(session_id) + " AND last_update_time > '"+str(start_time)+"' AND last_update_time < '"+str(end_time)+"' " \
                               "    GROUP BY object_id" \
                               ") mx ON rrp.object_id=mx.object_id and rrp.last_update_time=mx.last_update_time" \
                               " WHERE rrp.session_id = '"+str(session_id)+"' AND rrp.last_update_time > '"+str(start_time)+"' AND rrp.last_update_time < '"+str(end_time)+"' " \
                               "ORDER BY rrp.object_id"
            # print(query_rp)
            cur.execute(query_rp)
            data_rp = cur.fetchall()

            for rp in data_rp:

                object_id = rp[2]
                visibility_type = rp[7]
                rp_status = 'P' + str(object_id) #+'R' if visibility_type == 'REMOVE' else 'P'+str(object_id)
                rp_name             = rp[3]
                rp_latitude         = rp[4]
                rp_longitude         = rp[5]
                rp_altitude         = rp[6]
                rp_link_status      = rp[11]
                rp_amplification    = rp[8]
                rp_last_update_time = rp[12]
                if rp_status not in added_track:
                    added_track.append(rp_status)
                    rp_status = rp_status + 'A'
                else:
                    if visibility_type == 'REMOVE':
                        added_track.remove(rp_status)
                        rp_status = rp_status + 'R'
                    else:
                        rp_status = rp_status + 'U'
                # rp_track = [rp_status, rp_name, rp_latitude, rp_longitude, rp_altitude, rp_link_status, rp_amplification, str(rp_last_update_time)]
                rp_track = {
                    "object_type": rp[1],
                    "object_id": object_id, "system_track_number": rp_status,
                    "name": rp_name,
                    "latitude": rp_latitude,
                    "longitude": rp_longitude,
                    "altitude": rp_altitude,
                    "visibility_type": visibility_type,
                    "point_amplification_type": rp_amplification,
                    "is_editable": rp[9],
                    "network_track_number": rp[10],
                    "link_status_type": rp_link_status,
                    "last_update_time": str(rp_last_update_time)
                }
                track_data['data']['reference_point'].append(rp_track)

            query_aa = "SELECT  aa.* " \
                                " FROM area_alerts aa " \
                                " JOIN (" \
                                "    SELECT object_id,max(last_update_time) last_update_time " \
                                "    FROM area_alerts " \
                                "    WHERE session_id = '" + str(session_id) + "' AND last_update_time > '"+str(start_time)+"' AND last_update_time < '"+str(end_time)+"' " \
                                "    GROUP BY object_id " \
                                ") mx ON aa.object_id=mx.object_id and aa.last_update_time=mx.last_update_time " \
                                " WHERE aa.session_id = '" + str(session_id) + "' " \
                                 " AND aa.last_update_time > '"+str(start_time)+"' AND aa.last_update_time < '"+str(end_time)+"' " \
                                 " ORDER BY aa.object_id"
                    # query_aa = "SELECT aa.session_id as id, aa.*  FROM area_alerts aa  JOIN (    SELECT object_id,max(last_update_time) last_update_time     FROM area_alerts     WHERE session_id = '1' AND last_update_time > '2020-01-10 14:14:31' AND last_update_time < '2020-01-10 14:14:41'     GROUP BY object_id ) mx ON aa.object_id=mx.object_id and aa.last_update_time=mx.last_update_time  WHERE aa.session_id = '1'  AND aa.last_update_time > '2020-01-10 14:14:31' AND aa.last_update_time < '2020-01-10 14:14:41'  ORDER BY aa.object_id"
            cur.execute(query_aa)
            data_aa = cur.fetchall()
            for aa in data_aa:

                object_id = aa[2]

                object_type = aa[1]
                warning_type = aa[3]
                track_name = aa[4]
                last_update_time = aa[5]
                mmsi_number = aa[6]
                ship_name = aa[7]
                track_source_type = aa[8]
                aa_status = 'AA' + str(object_id) #+'R' if is_visible == 'REMOVE' else 'AA'+str(object_id)
                if aa_status not in added_track:
                    added_track.append(aa_status)
                    aa_status = aa_status + 'A'
                else:
                    if is_visible == 'REMOVE':
                        added_track.remove(aa_status)
                        aa_status = aa_status + 'R'
                    else:
                        aa_status = aa_status + 'U'
                aa_track = {"system_track_number": aa_status,
                            "object_type": object_type,
                            "object_id": object_id,
                            "warning_type": warning_type,
                            "track_name": track_name,
                            "last_update_time": str(last_update_time),
                            "mmsi_number": mmsi_number,
                            "ship_name": ship_name,
                            "track_source_type": track_source_type,
                            "is_visible": is_visible}

                track_data['data']['area_alert'].append(aa_track)

            result['track_play'][t] = track_data

        track.append(result)
        q_store_replay = "INSERT INTO stored_replay(update_rate, session_id, data)" \
                         "	VALUES ("+str(UPDATE_RATE)+", "+str(session_id)+", '"+json.dumps(track)+"' );"
        cur.execute(q_store_replay)
        conn.commit()

        # print(q_store_replay)

    replay_data_send.append(result)
    print(json.dumps(replay_data_send))

if __name__ == "__main__":
    
    query = "SELECT id FROM sessions where end_time is not null";
    cur.execute(query)
    session = cur.fetchall()
    if(len(session)>0):
        print("checking replay")
        for s in session:
            query = "SELECT * FROM stored_replay WHERE session_id="+str(s[0])+" AND update_rate= "+str(UPDATE_RATE)+" "
            print(query)
            cur.execute(query)
            recorded = cur.fetchall()
            if(len(recorded) == 0):
                print("generating replay")
                get_replay()




