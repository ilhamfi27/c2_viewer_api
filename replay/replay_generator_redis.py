from main import *


replay_data_send = []

def replay_track(session_id, start_time, end_time, added_track, data_lengkap, data_ais):
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
    just_added = []
    i = 0
    for table in ar_mandatory_table:
        
        # column = "st.system_track_number, st.track_phase_type" if table == 'replay_system_track_processing' else "st.system_track_number"
        sql_mandatory = "SELECT * \
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
        # print(t, sql_mandatory)
        cur.execute(sql_mandatory)
        data_mandatory = cur.fetchall()

        if len(data_mandatory) > 0:
            for d in data_mandatory:
                # if d[0] not in data_lengkap[i]:
                if table == 'replay_system_track_general':
                    if str(d[10]) == 'AIS_TYPE':
                        q_ais_data = "SELECT  * \
                                        FROM  \
                                        ( \
                                            SELECT * \
                                                FROM replay_ais_data  \
                                            WHERE session_id = " + str(session_id) + "   \
                                            AND system_track_number = " + str(d[1]) + "  \
                                                AND created_time > '" + start_time + "'  \
                                                AND created_time < '" + end_time + "'  \
                                            ORDER BY created_time DESC  \
                                        ) aa LIMIT 1;"                                                         
                        cur.execute(q_ais_data)
                        ais_data = cur.fetchall()
                        
                        if len(ais_data) > 0:                            
                            data_lengkap[i][d[1]] = d 
                            redis_ais_key = "AIS"+str(d[1])
                            r.set(str(redis_ais_key), str(ais_data[0]))
                            data_ais[d[1]] = [ais_data[0]]
                            # print(data_lengkap[0][d[1]])

                            
                        conn.commit()
                    else:
                        data_lengkap[0][d[1]] = d
                else:
                    data_lengkap[i][d[1]] = d
                
        conn.commit()
                    # if d[0] not in just_added:
                    #     just_added.append(d[0])
        i = i +1

    data_lengkap_index    = [[],[],[]]
    [data_lengkap_index[i].append(key) for i in range(len(data_lengkap)) for key, value in data_lengkap[i].items()]
    data_ready          = reduce(np.intersect1d, data_lengkap_index)
    # just_completed      = np.intersect1d(data_ready, just_added)
    # print(just_completed, start_time, end_time)
    # print(data_ready, start_time, end_time)
        # print(data_ready)
    track_final = []
    # print(data_ready)
    if len(data_ready) > 0:  
            
        for ready in data_ready:            
            track = {}
            recorded_track  = {}                          
            
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
                table_data = cur.fetchall()                      
                t_status = "T" + str(ready)                            
                if table is 'replay_system_track_general':
                    # print(table, ready, is_exist)
                    # print(sql_track)
                    # print(ready, len(table_data))
                    if len(table_data) == 0:
                        table_data = [data_lengkap[0][ready]]
                        
                    #     print(table_data) 
                    # else:
                    #     print(table_data) 
                    for td in table_data:                       
                        track['created_time']        = str(td[20])
                        track['source_data']         = str(td[10])
                        track['track_name']          = str(td[2])
                        track['environment']         = str(td[5])
                        track['iu_indicator']        = str(td[12])
                        track['identity']            = str(td[4])
                        track['initiation_time']     = str(td[19])
                        track['airbone_indicator']   = str(td[21])
                        if str(track['source_data']) == 'AIS_TYPE':
                            q_ais_data = "SELECT  * \
                                            FROM  \
                                            ( \
                                            SELECT * \
                                                FROM replay_ais_data  \
                                            WHERE session_id = " + str(session_id) + "   \
                                            AND system_track_number = " + str(td[1]) + "  \
                                                AND created_time > '" + start_time + "'  \
                                                AND created_time < '" + end_time + "'  \
                                            ORDER BY created_time DESC  \
                                            ) aa LIMIT 1;"                                    
                            cur.execute(q_ais_data)
                            ais_data = []                        
                           
                            if len(ais_data) > 0:
                                ais_data = cur.fetchall()
                            else:
                                # print(data_ais[ready])
                                # ais_data = [r.get("AIS"+str(ready))]
                                ais_data = data_ais[ready]
                            
                            for ais in ais_data:
                                    track['mmsi_number']            = str(ais[2])
                                    track['ship_name']              = str(ais[3])
                                    track['radio_call_sign']        = str(ais[4])
                                    track['imo_number']             = str(ais[5])
                                    track['navigation_status']      = str(ais[6])
                                    track['destination']            = str(ais[7])
                                    track['dimension_of_ship']      = str(ais[8])
                                    track['ship_type']              = str(ais[9])
                                    track['rate_of_turn']           = str(ais[10])
                                    track['gross_tonnage']          = str(ais[12])
                                    track['country']                = str(ais[13])
                                    track['eta']                    = str(ais[15])
                                    track['vendor_id']              = str(ais[16])
                        else:
                            track['created_time']        = str(td[1])
                            track['source_data']         = str(td[10])
                            track['track_name']          = str(td[2])
                            track['environment']         = str(td[5])
                            track['iu_indicator']        = str(td[12])
                            track['identity']            = str(td[4])
                            track['initiation_time']     = str(td[19])
                            track['airbone_indicator']   = str(td[21])
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
                            mission_data = cur.fetchall()  
                            for md in mission_data:                      
                                track['mission_name']            = str(md[2])
                                track['mission_route']           = str(md[3])
                                track['voice_call_sig']          = str(md[4])
                                track['voice_frequency_channel'] = str(md[5])
                                track['fuel_status']             = str(md[6])
                                track['mission_start']           = str(md[8])
                                track['mission_finish']          = str(md[9])
                                
                if table == 'replay_system_track_processing':
                    if len(table_data) == 0:
                        table_data = [data_lengkap[2][ready]]
                    for td in table_data:   
                        track['fusion_status']       = str(td[2])
                        track['join_status']         = str(td[3])
                        track['track_phase']         = str(td[5])
                        track['suspect_level']       = str(td[6])                        
                        track_status = "T" + str(ready)
                        if track_status not in added_track:
                            track["track_status"] = track_status+ "A"
                            added_track.append(track_status)
                        else:
                            if track['track_phase'] in ['DELETED_BY_SYSTEM', 'DELETED_BY_SENSOR']: 
                                added_track.remove(track_status)                                
                                for track_lengkap in data_lengkap: 
                                    if ready in track_lengkap:                                  
                                        del track_lengkap[ready]
                            else:
                                track["track_status"] = track_status + "U"
                if table == 'replay_system_track_kinetic':
                    if len(table_data) == 0:
                        table_data = [data_lengkap[1][ready]]
                    for td in table_data:   
                        track['heading']             = str(td[3])
                        track['latitude']            = str(td[4])
                        track['longitude']           = str(td[5])
                        track['altitude']            = str(td[8])
                        track['speed_over_ground']   = str(td[9])
                        track['course_over_ground']  = str(td[10])
                        track['last_update_time']    = str(td[11])                        
                    
                if table == 'replay_system_track_link':
                    for td in table_data:   
                        track['network_track_number']    = str(td[3])
                        track['originator']              = str(td[5])
                        track['link_status']             = str(td[8])
                        # track['created_time']            = str(d[9])                
                                    
                if table == 'replay_system_track_identification':
                    for td in table_data:   
                        track['environment'] = td[3]
                        if td[3] == 'AIR':
                            track['platform_type']       = str(td[4])
                            track['platform_activity']   = str(td[7])
                            track['specific_type']       = str(td[10])
                        elif td[3] == 'SURFACE':
                            track['platform_type']       = str(td[5])
                            track['platform_activity']   = str(td[8])
                            track['specific_type']       = str(td[11])
                        else:
                            track['platform_type']       = str(td[6])
                            track['platform_activity']   = str(td[9])
                            track['specific_type']       = str(td[12])                            
                    # if t_status not in recorded_track:
                    #     recorded_track[t_status] = str(created_time)
                    # else:
                    #     if created_time > str(recorded_track[t_status]):
                    #         recorded_track[t_status] = str(created_time)
            ''' Check Hash, kalau beda baru append'''
            redis_key           = str(session_id) + "T" + str(ready)
            redis_value         = reduce(concat, track.values()) 
            hashed_redis_value  = hashlib.md5(redis_value.encode('utf-8')).hexdigest()
            track['hashed']     = hashed_redis_value
            if r.exists(str(redis_key)):
                data_from_hashmap = r.get(str(redis_key))
                if data_from_hashmap != hashed_redis_value:
                    # print(ready, data_from_hashmap)
                    track_final.append(track)
                    r.set(str(redis_key), hashed_redis_value)
            else:
                r.set(str(redis_key), hashed_redis_value)
                track_final.append(track)

            
    return_data.append(track_final)
    return_data.append(added_track)
    return_data.append(data_lengkap)
    return_data.append(data_ais)
    
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
        data_lengkap = [{},{},{}]
        ais_data    = {}
        session_id  = data[0]
        start_time  = data[1]
        end_time    = data[2]
        durasi      = data[3]
        name        = data[4]
        '''Buat panjang durasi dibagi dengan UPDATE_RATE. Buat list sesuai dengan panjang_replay'''
        panjang_replay = durasi / UPDATE_RATE
        track_list = [i for i in range(int(panjang_replay))]        
        track_list = dict.fromkeys(track_list, {})
        result={
                        "session_id"        : str(session_id),
                        "start_time"        : str(start_time),
                        "end_time"          : str(end_time),
                        "session_name"      : str(name),
                        "update_rate"       : str(UPDATE_RATE),
                        "durasi_session"    : str(durasi),
                        "track_play"        : track_list
                }
        # print(result)
        start_time  = (datetime.strptime(str(start_time), '%Y-%m-%d %H:%M:%S'))
        end_time    = (datetime.strptime(str(end_time), '%Y-%m-%d %H:%M:%S'))
        added_track = []
        
        '''Looping sebanyak panjang replay'''
        for t in range(len(track_list)):
            
            
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
            
            result["track_play"][str(t)] = {
                                "start_time"        : str(start_time),
                                "end_time"          : str(end_time),
                                "track"             : [],
                                "reference_point"   : [],
                                "tactical_figures"  : [],
                                "area_alert"        : []

            }
            '''Jalankan query untuk setiap tabel per setiap segmen durasi'''

            track_replay_data = replay_track(session_id, str(start_time), str(end_time), added_track, data_lengkap, ais_data)
            result["track_play"][str(t)]["track"].append(track_replay_data[0])            
            added_track = track_replay_data[1]
            data_lengkap = track_replay_data[2]
            ais_data = track_replay_data[3]
            # print(t, len(data_lengkap[0]))
            # print(result["track_play"], start_time, end_time)
            # print(track_replay_data[1])
            # for i in track_replay_data[1]:
            #     if i not in added_track :
            #         added_track.append(i)
            # added_track.extend(track_replay_data[1])
            # track_data.append(track_replay_data)
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
                object_id                   = str(tf[2])
                is_visible                  = str(tf[8])
                tf_name                     = str(tf[3])
                tf_environment              = str(tf[4])
                tf_ntn                      = str(tf[10])
                tf_link_status              = str(tf[11])
                tf_status                   = str('F'+str(object_id))
                tf_amplification            = str(tf[13])
                tf_last_update_time         = str(tf[15])
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
                            "object_type": str(tf[1]),
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
                result["track_play"][str(t)]["tactical_figures"].append(tf_track)  
            conn.commit()              

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
                rp_track            = {}
                object_id           = rp[2]
                visibility_type     = rp[7]
                rp_status           = 'P' + str(object_id)
                rp_name             = str(rp[3])
                rp_latitude         = str(rp[4])
                rp_longitude        = str(rp[5])
                rp_altitude         = str(rp[6])
                rp_link_status      = str(rp[11])
                rp_amplification    = str(rp[8])
                rp_last_update_time = str(rp[12])
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
                rp_track["object_type"] = rp[1]
                rp_track["object_id"] =  object_id
                rp_track["system_track_number"] =  rp_status
                rp_track["name"] = rp_name
                rp_track["latitude"] = rp_latitude
                rp_track["longitude"] = rp_longitude
                rp_track["altitude"] = rp_altitude
                rp_track["visibility_type"] = visibility_type
                rp_track["point_amplification_type"] = rp_amplification
                rp_track["is_editable"] = rp[9]
                rp_track["network_track_number"] = rp[10]
                rp_track["link_status_type"] = rp_link_status
                rp_track["last_update_time"] = str(rp_last_update_time)
                # print(rp_track)
                result["track_play"][str(t)]["reference_point"].append(rp_track)  
            conn.commit()              
                

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
                object_id           = str(aa[2])
                object_type         = str(aa[1])
                warning_type        = str(aa[3])
                track_name          = str(aa[4])
                last_update_time    = str(aa[5])
                mmsi_number         = str(aa[6])
                ship_name           = str(aa[7])
                track_source_type   = str(aa[8])
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
                
                result["track_play"][str(t)]["area_alert"].append(aa_track)      
            conn.commit()

        track.append(result)
        check_stored = "SELECT * FROM stored_replay WHERE session_id="+str(session_id)+" AND update_rate="+str(UPDATE_RATE)+" "
        cur.execute(check_stored)
        q_store_replay = "INSERT INTO stored_replay(update_rate, session_id, data)" \
                            "	VALUES ("+str(UPDATE_RATE)+", "+str(session_id)+", '"+str(json.dumps(result))+"' )"
        data = cur.fetchall()
        if len(data) == 0:            
            cur.execute(q_store_replay)
            conn.commit()
        print("Done")
        # print(q_store_replay)
    # print(json.dumps(result))
    replay_data_send.append(result)
    # print(replay_data_send)
    
    # print(json.dumps(replay_data_send, default=str))

    conn.commit()
    
