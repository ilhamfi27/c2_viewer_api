#!/usr/bin/env python

# WS server example that synchronizes REALTIME_state across clients
from main import *


USERS = set()
replay_data_send = []
done_generate    = []
sent_before      = {}
def replay_track(session_id, start_time, end_time, data_track, added_track):
    
    # session_id = 19 if 19 not in done_generate and session_id<=19 else 20    
    return_data = []
    track_final = {}
    track_data = []
    changed_mandatory_data = {
                'replay_system_track_kinetic'       : {},
                'replay_system_track_processing'    : {}
    }
    ar_mandatory_table_8 = [            
        'replay_system_track_identification',
        'replay_system_track_link',        
        'replay_track_general_setting',
    ]
    ar_mandatory_table = [
        'replay_system_track_general',
        'replay_system_track_kinetic',
        'replay_system_track_processing'
    ]
    
        # print(session_id, start_time, end_time, data_track)
    for table in ar_mandatory_table:                
        sql_mandatory = "SELECT * \
                                FROM " + table + " st \
                                JOIN( \
                                    SELECT system_track_number,max(created_time) created_time \
                                    FROM " + table + " \
                                    WHERE session_id = " + str(session_id) + " AND created_time >= '" + str(
                    start_time) + "' AND created_time <= '" + str(end_time) + "' \
                                    GROUP BY system_track_number \
                                ) mx ON st.system_track_number=mx.system_track_number and st.created_time=mx.created_time \
                                WHERE st.session_id = " + str(session_id) + " AND st.created_time >= '" + str(
                    start_time) + "' AND st.created_time <= '" + str(end_time) + "' \
                                ORDER BY st.system_track_number"                
        cur.execute(sql_mandatory)
        data_mandatory = cur.fetchall()

        if len(data_mandatory) > 0:
            for d in data_mandatory:
                system_track_number = str(d[1])
                if system_track_number not in data_track:
                    data_track[system_track_number] = {}
                if table not in data_track[system_track_number]:
                    data_track[system_track_number][table] = {}
                if table == 'replay_system_track_general':
                    data_track[system_track_number][table]['created_time']        = str(d[20])
                    data_track[system_track_number][table]['source_data']         = str(d[10])
                    data_track[system_track_number][table]['track_name']          = str(d[2])
                    data_track[system_track_number][table]['environment']         = str(d[5])
                    data_track[system_track_number][table]['iu_indicator']        = str(d[12])
                    data_track[system_track_number][table]['identity']            = str(d[4])
                    data_track[system_track_number][table]['initiation_time']     = str(d[19])
                    data_track[system_track_number][table]['airborne_indicator']   = str(d[21])
                    table_value         = reduce(concat, data_track[system_track_number]['replay_system_track_general'].values())
                    hashed_value  = hashlib.md5(table_value.encode('utf-8')).hexdigest()
                    data_track[system_track_number]['replay_system_track_general']['hash'] = hashed_value

                    if data_track[system_track_number][table]['source_data'] == 'AIS_TYPE':
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
                            if 'replay_ais_data' not in data_track[system_track_number]:
                                data_track[system_track_number]['replay_ais_data'] = {}
                            for ais in ais_data:
                                data_track[system_track_number]['replay_ais_data']['mmsi_number']            = str(ais[2])
                                data_track[system_track_number]['replay_ais_data']['ship_name']              = str(ais[3])
                                data_track[system_track_number]['replay_ais_data']['radio_call_sign']        = str(ais[4])
                                data_track[system_track_number]['replay_ais_data']['imo_number']             = str(ais[5])
                                data_track[system_track_number]['replay_ais_data']['navigation_status']      = str(ais[6])
                                data_track[system_track_number]['replay_ais_data']['destination']            = str(ais[7])
                                data_track[system_track_number]['replay_ais_data']['dimension_of_ship']      = str(ais[8])
                                data_track[system_track_number]['replay_ais_data']['ship_type']              = str(ais[9])
                                data_track[system_track_number]['replay_ais_data']['rate_of_turn']           = str(ais[10])
                                data_track[system_track_number]['replay_ais_data']['gross_tonnage']          = str(ais[12])
                                data_track[system_track_number]['replay_ais_data']['country']                = str(ais[13])
                                data_track[system_track_number]['replay_ais_data']['eta']                    = str(ais[15])
                                data_track[system_track_number]['replay_ais_data']['vendor_id']              = str(ais[16])
                                table_value         = reduce(concat, data_track[system_track_number][table].values())
                                hashed_value  = hashlib.md5(table_value.encode('utf-8')).hexdigest()
                                data_track[system_track_number]['replay_ais_data']['hash'] = hashed_value
                                
                elif table == 'replay_system_track_kinetic':
                    data_track[system_track_number]['replay_system_track_kinetic']['heading']             = str(d[3])
                    data_track[system_track_number]['replay_system_track_kinetic']['latitude']            = str(d[4])
                    data_track[system_track_number]['replay_system_track_kinetic']['longitude']           = str(d[5])
                    data_track[system_track_number]['replay_system_track_kinetic']['altitude']            = str(d[8])
                    data_track[system_track_number]['replay_system_track_kinetic']['speed_over_ground']   = str(d[9])
                    data_track[system_track_number]['replay_system_track_kinetic']['course_over_ground']  = str(d[10])
                    data_track[system_track_number]['replay_system_track_kinetic']['last_update_time']    = str(d[11])
                    table_value         = reduce(concat, data_track[system_track_number]['replay_system_track_kinetic'].values())
                    hashed_value  = hashlib.md5(table_value.encode('utf-8')).hexdigest()
                    if 'hash' in data_track[system_track_number]['replay_system_track_kinetic']:
                        stored_kinetic_hash = data_track[system_track_number]['replay_system_track_kinetic']['hash']
                        if stored_kinetic_hash != hashed_value:
                            changed_mandatory_data['replay_system_track_kinetic'][system_track_number] = data_track[system_track_number]['replay_system_track_kinetic']
                    data_track[system_track_number]['replay_system_track_kinetic']['hash'] = hashed_value
                    
                    

                else:
                    data_track[system_track_number][table]['fusion_status']       = str(d[2])
                    data_track[system_track_number][table]['join_status']         = str(d[3])
                    data_track[system_track_number][table]['track_phase_type']    = str(d[5])
                    data_track[system_track_number][table]['suspect_level']       = str(d[6])
                    table_value         = reduce(concat, data_track[system_track_number]['replay_system_track_processing'].values())
                    hashed_value  = hashlib.md5(table_value.encode('utf-8')).hexdigest()
                    if 'hash' in data_track[system_track_number]['replay_system_track_processing']:
                        stored_processing_hash = data_track[system_track_number]['replay_system_track_processing']['hash']
                        if stored_processing_hash != hashed_value:
                            changed_mandatory_data['replay_system_track_processing'][system_track_number] = data_track[system_track_number]['replay_system_track_processing']
                    data_track[system_track_number]['replay_system_track_processing']['hash'] = hashed_value
                    
                

    
    for stn, data in data_track.items():  
        
        if 'replay_system_track_general' in data and \
            data['replay_system_track_general']['source_data'] == 'AIS_TYPE' and \
            'replay_ais_data'not in data :
            q_ais_data = "SELECT  * \
                                                FROM  \
                                                ( \
                                                SELECT * \
                                                    FROM replay_ais_data  \
                                                WHERE session_id = " + str(session_id) + "   \
                                                AND system_track_number = " + str(stn) + "  \
                                                    AND created_time >= '" + start_time + "'  \
                                                    AND created_time <= '" + end_time + "'  \
                                                ORDER BY created_time DESC  \
                                                ) aa LIMIT 1;"
            cur.execute(q_ais_data)
            ais_data = cur.fetchall()
            if len(ais_data) > 0:
                if 'replay_ais_data' not in data_track[stn]:
                    data_track[stn]['replay_ais_data'] = {}
                for ais in ais_data:
                    data_track[stn]['replay_ais_data']['mmsi_number']            = str(ais[2])
                    data_track[stn]['replay_ais_data']['ship_name']              = str(ais[3])
                    data_track[stn]['replay_ais_data']['radio_call_sign']        = str(ais[4])
                    data_track[stn]['replay_ais_data']['imo_number']             = str(ais[5])
                    data_track[stn]['replay_ais_data']['navigation_status']      = str(ais[6])
                    data_track[stn]['replay_ais_data']['destination']            = str(ais[7])
                    data_track[stn]['replay_ais_data']['dimension_of_ship']      = str(ais[8])
                    data_track[stn]['replay_ais_data']['ship_type']              = str(ais[9])
                    data_track[stn]['replay_ais_data']['rate_of_turn']           = str(ais[10])
                    data_track[stn]['replay_ais_data']['gross_tonnage']          = str(ais[12])
                    data_track[stn]['replay_ais_data']['country']                = str(ais[13])
                    data_track[stn]['replay_ais_data']['eta']                    = str(ais[15])
                    data_track[stn]['replay_ais_data']['vendor_id']              = str(ais[16])
                    table_value         = reduce(concat, data_track[stn][table].values())
                    hashed_value        = hashlib.md5(table_value.encode('utf-8')).hexdigest()
                    data_track[stn]['replay_ais_data']['hash'] = hashed_value
        if 'replay_system_track_general' in data and \
                'replay_system_track_kinetic' in data and \
                'replay_system_track_processing' in data:
            if data['replay_system_track_general']['source_data'] == 'AIS_TYPE':                        
                if 'replay_ais_data' in data:
                    data['mandatory_complete_status'] = True
                else:
                    data['mandatory_complete_status'] = False
            else:
                data['mandatory_complete_status'] = True  
        else:
            data['mandatory_complete_status'] = False 

    
    for key, value in data_track.items():           
        track           = {}
        recorded_track  = {}
        if session_id not in done_generate:
            if value['mandatory_complete_status']:  
                
                if key not in added_track:
                    track['track_status'] = "T" + str(key) + "A"
                    #general
                    track['created_time']        = str(value['replay_system_track_general']['created_time'])
                    track['source_data']         = str(value['replay_system_track_general']['source_data'])
                    track['track_name']          = str(value['replay_system_track_general']['track_name'])
                    track['environment']         = str(value['replay_system_track_general']['environment'])
                    track['iu_indicator']        = str(value['replay_system_track_general']['iu_indicator'])
                    track['identity']            = str(value['replay_system_track_general']['identity'])
                    track['initiation_time']     = str(value['replay_system_track_general']['initiation_time'])
                    track['airborne_indicator']   = str(value['replay_system_track_general']['airborne_indicator'])
                    if track['source_data'] == 'AIS_TYPE':                        
                        track['mmsi_number']            = str(value['replay_ais_data']['mmsi_number'])
                        track['ship_name']              = str(value['replay_ais_data']['ship_name'])
                        track['radio_call_sign']        = str(value['replay_ais_data']['radio_call_sign'])
                        track['imo_number']             = str(value['replay_ais_data']['imo_number'])
                        track['navigation_status']      = str(value['replay_ais_data']['navigation_status'])
                        track['destination']            = str(value['replay_ais_data']['destination'])
                        track['dimension_of_ship']      = str(value['replay_ais_data']['dimension_of_ship'])
                        track['ship_type']              = str(value['replay_ais_data']['ship_type'])
                        track['rate_of_turn']           = str(value['replay_ais_data']['rate_of_turn'])
                        track['gross_tonnage']          = str(value['replay_ais_data']['gross_tonnage'])
                        track['country']                = str(value['replay_ais_data']['country'])
                        track['eta']                    = str(value['replay_ais_data']['eta'])
                        track['vendor_id']              = str(value['replay_ais_data']['vendor_id'])
                    if track['iu_indicator'] :
                        sql_mission = "SELECT * FROM replay_system_track_mission st \
                                        JOIN (" \
                                            "SELECT system_track_number,max(created_time) created_time " \
                                            "FROM replay_system_track_mission " \
                                            "WHERE session_id = '" + str(session_id) + "' \
                                            AND created_time >= '" + start_time + "' AND created_time < '" + end_time + "' \
                                            GROUP BY system_track_number \
                                        ) mx ON st.system_track_number = mx.system_track_number and st.created_time = mx.created_time \
                                        WHERE st.session_id = " + str(session_id) + " " \
                                        "AND st.created_time >= '" + start_time + "' AND st.created_time < '" + end_time + "' \
                                        AND st.system_track_number = " + str(key) + " \
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

                            if 'replay_system_track_mission' not in value:
                                value['replay_system_track_mission'] = {}
                            value['replay_system_track_mission'] = {}
                            value['replay_system_track_mission']['mission_name']            = str(md[2])
                            value['replay_system_track_mission']['mission_route']           = str(md[3])
                            value['replay_system_track_mission']['voice_call_sig']          = str(md[4])
                            value['replay_system_track_mission']['voice_frequency_channel'] = str(md[5])
                            value['replay_system_track_mission']['fuel_status']             = str(md[6])
                            value['replay_system_track_mission']['mission_start']           = str(md[8])
                            value['replay_system_track_mission']['mission_finish']          = str(md[9])
                            mission_value         = reduce(concat, value['replay_system_track_mission'].values())
                            hashed_mission_value  = hashlib.md5(mission_value.encode('utf-8')).hexdigest()
                            value['replay_system_track_mission']['hash'] = hashed_mission_value

                    #kinetic
                    track['heading']             = str(value['replay_system_track_kinetic']['heading'])
                    track['latitude']            = str(value['replay_system_track_kinetic']['latitude'])
                    track['longitude']           = str(value['replay_system_track_kinetic']['longitude'])
                    track['altitude']            = str(value['replay_system_track_kinetic']['altitude'])
                    track['speed_over_ground']   = str(value['replay_system_track_kinetic']['speed_over_ground'])
                    track['course_over_ground']  = str(value['replay_system_track_kinetic']['course_over_ground'])
                    track['last_update_time']    = str(value['replay_system_track_kinetic']['last_update_time'])
                    #processing
                    track['fusion_status']       = str(value['replay_system_track_processing']['fusion_status'])
                    track['join_status']         = str(value['replay_system_track_processing']['join_status'])
                    track['track_phase_type']    = str(value['replay_system_track_processing']['track_phase_type'])
                    track['suspect_level']       = str(value['replay_system_track_processing']['suspect_level'])
                else:           
                    if key in changed_mandatory_data['replay_system_track_kinetic']:
                        changed_kinetic = changed_mandatory_data['replay_system_track_kinetic'][key]
                        # print(key, changed_kinetic)
                        track['heading']             = str(changed_kinetic['heading'])
                        track['latitude']            = str(changed_kinetic['latitude'])
                        track['longitude']           = str(changed_kinetic['longitude'])
                        track['altitude']            = str(changed_kinetic['altitude'])
                        track['speed_over_ground']   = str(changed_kinetic['speed_over_ground'])
                        track['course_over_ground']  = str(changed_kinetic['course_over_ground'])
                        track['last_update_time']    = str(changed_kinetic['last_update_time'])                       
                        track['hash']                = changed_kinetic['hash']
                        track['track_status']        = "T" + str(key) + "U"                    

                    # print(value['replay_system_track_general'])     
                    if value['replay_system_track_general']['iu_indicator']:
                        sql_mission = "SELECT * FROM replay_system_track_mission st \
                                        JOIN (" \
                                            "SELECT system_track_number,max(created_time) created_time " \
                                            "FROM replay_system_track_mission " \
                                            "WHERE session_id = '" + str(session_id) + "' \
                                            AND created_time >= '" + start_time + "' AND created_time < '" + end_time + "' \
                                            GROUP BY system_track_number \
                                        ) mx ON st.system_track_number = mx.system_track_number and st.created_time = mx.created_time \
                                        WHERE st.session_id = " + str(session_id) + " " \
                                        "AND st.created_time >= '" + start_time + "' AND st.created_time < '" + end_time + "' \
                                        AND st.system_track_number = " + str(key) + " \
                                        ORDER BY st.system_track_number"    
                        cur.execute(sql_mission)
                        mission_data = cur.fetchall()
                        for md in mission_data:
                            new_mission = {}
                            new_mission['mission_name']            = str(md[2])
                            new_mission['mission_route']           = str(md[3])
                            new_mission['voice_call_sig']          = str(md[4])
                            new_mission['voice_frequency_channel'] = str(md[5])
                            new_mission['fuel_status']             = str(md[6])
                            new_mission['mission_start']           = str(md[8])
                            new_mission['mission_finish']          = str(md[9])    

                            new_mission_value         = reduce(concat, new_mission.values())
                            new_mission_hashed_value  = hashlib.md5(new_mission_value.encode('utf-8')).hexdigest()
                            stored_mission_hash = data_track[key]['replay_system_track_mission']['hash']                        
                            if stored_mission_hash != new_mission_hashed_value:                            
                                track['mission_name']            = str(new_mission['mission_name'])
                                track['mission_route']           = str(new_mission['mission_route'])
                                track['voice_call_sig']          = str(new_mission['voice_call_sig'])
                                track['voice_frequency_channel'] = str(new_mission['voice_frequency_channel'])
                                track['fuel_status']             = str(new_mission['fuel_status'])
                                track['mission_start']           = str(new_mission['mission_start'])
                                track['mission_finish']          = str(new_mission['mission_finish'])
                                data_track[key]['replay_system_track_mission']['hash'] = new_mission_hashed_value
                                track['track_status'] = "T" + str(key) + "U"

                    if key in changed_mandatory_data['replay_system_track_processing']:
                        changed_processing = changed_mandatory_data['replay_system_track_processing'][key]
                        track['fusion_status']       = str(changed_processing['fusion_status'])
                        track['join_status']         = str(changed_processing['join_status'])
                        track['track_phase_type']    = str(changed_processing['track_phase_type'])
                        track['suspect_level']       = str(changed_processing['suspect_level'])
                        if track['track_phase_type'] in ['DELETED_BY_SYSTEM', 'DELETED_BY_SENSOR']:
                            value['replay_system_track_processing'].clear()
                            data_track[key].clear()
                            track['track_status']        = "T" + str(key) + "R"
                            value['mandatory_complete_status'] = False
                            added_track.remove(key)
                        else:
                            track['track_status']        = "T" + str(key) + "U"
                                                       
                for table in ar_mandatory_table_8:
                    sql_track = "SELECT * FROM " + table + " st \
                                JOIN (" \
                                    "SELECT system_track_number,max(created_time) created_time " \
                                    "FROM " + table + " " \
                                    "WHERE session_id = '" + str(session_id) + "' \
                                    AND created_time >= '" + start_time + "' AND created_time <= '" + end_time + "' \
                                    GROUP BY system_track_number \
                                ) mx ON st.system_track_number = mx.system_track_number and st.created_time = mx.created_time \
                                WHERE st.session_id = " + str(session_id) + " " \
                                "AND st.created_time >= '" + start_time + "' AND st.created_time <= '" + end_time + "' \
                                AND st.system_track_number = " + str(key) + " \
                                ORDER BY st.system_track_number"
                    cur.execute(sql_track)
                    table_data = cur.fetchall()
                    t_status = "T" + str(key)
                    if len(table_data) > 0 :
                        if table not in value:
                            value[table] = {}   
                        if table == 'replay_system_track_link':
                            for td in table_data:
                                value[table]['network_track_number']    = str(td[3])
                                value[table]['originator']              = str(td[5])
                                value[table]['link_status']             = str(td[8])
                                
                                track['network_track_number']    = str(td[3])
                                track['originator']              = str(td[5])
                                track['link_status']             = str(td[8])
                        if table == 'replay_system_track_identification':
                            for td in table_data:
                                value['environment'] = td[3]
                                
                                value[table]['air_platform']       = str(td[4])
                                value[table]['air_platform_activity']   = str(td[7])
                                value[table]['air_specific']       = str(td[10])
                                
                                value[table]['surf_platform']       = str(td[5])
                                value[table]['surf_platform_activity']   = str(td[8])
                                value[table]['surf_specific']       = str(td[11])
                                
                                value[table]['land_platform']       = str(td[6])
                                value[table]['land_platform_activity']   = str(td[9])
                                value[table]['land_specific']       = str(td[12]) 

                                track['air_platform']               = str(td[4])
                                track['air_platform_activity']      = str(td[7])
                                track['air_specific']               = str(td[10])

                                track['surf_platform']              = str(td[5])
                                track['surf_platform_activity']     = str(td[8])
                                track['surf_specific']              = str(td[11])

                                track['land_platform']              = str(td[6])
                                track['land_platform_activity']          = str(td[9])
                                track['land_specific']                   = str(td[12])

                                
                track_final[key] = {}
                track_final[key] = track
                if key not in added_track:
                    added_track.append(key)
    # print(track_final)
    return_data.append(track_final)
    return_data.append(data_track)
    return_data.append(added_track)
    
    # return_data.append(check_ais_later)

    return return_data

# q = "SELECT aa.session_id as id, aa.*  FROM area_alerts aa  JOIN (    SELECT object_id,max(last_update_time) last_update_time     FROM area_alerts     WHERE session_id = '1' AND last_update_time > '2020-01-10 14:14:31' AND last_update_time < '2020-01-10 14:14:41'     GROUP BY object_id ) mx ON aa.object_id=mx.object_id and aa.last_update_time=mx.last_update_time  WHERE aa.session_id = '1'  AND aa.last_update_time > '2020-01-10 14:14:31' AND aa.last_update_time < '2020-01-10 14:14:41'  ORDER BY aa.object_id"
async def get_replay():
    '''Get data session yang sudah selesai'''
    sql = "select id, to_char (start_time::timestamp, 'YYYY-MM-DD HH24:MI:SS') start_time, " \
                  " to_char (end_time::timestamp, 'YYYY-MM-DD HH24:MI:SS') end_time, " \
                  "extract(epoch from (end_time::timestamp - start_time::timestamp)) as durasi, name " \
                  " from sessions " \
                  "WHERE end_time IS NOT null AND id=19"

    cur.execute(sql)
    query = cur.fetchall()
    track = []
    done_generate.clear()
    r.flushdb()

    for data in query:

        data_track = {}
        
        session_id  = data[0]
        start_time  = data[1]
        end_time    = data[2]
        durasi      = data[3]
        name        = data[4]
        '''Buat panjang durasi dibagi dengan UPDATE_RATE. Buat list sesuai dengan panjang_replay'''
        panjang_replay = durasi / UPDATE_RATE
        track_list_prep = [i for i in range(int(panjang_replay))]
        track_list = dict.fromkeys(track_list_prep, {})
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
        for t in track_list_prep:

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

            track_replay_data       = replay_track(session_id, str(start_time), str(end_time), data_track, added_track)
            result["track_play"][str(t)]["track"].append(track_replay_data[0])
            
            data_track = track_replay_data[1]
            added_track = track_replay_data[2]
            # ais_data = track_replay_data[3]
            # ais_data = track_replay_data[3]
            # check_ais_later = track_replay_data[4]
            query_tf = "SELECT tf.* " \
                                   "FROM tactical_figures tf " \
                                    "JOIN(" \
                                   "     SELECT object_id,max(last_update_time) last_update_time " \
                                   "     FROM tactical_figures " \
                                   "     WHERE session_id = " + str(session_id) + " AND last_update_time >= '"+str(start_time)+"' AND last_update_time < '"+str(end_time)+"' " \
                                    "     GROUP BY object_id) mx " \
                                    "ON tf.object_id=mx.object_id and tf.last_update_time=mx.last_update_time " \
                                    "WHERE tf.session_id = '"+str(session_id)+"' AND tf.last_update_time >= '"+str(start_time)+"' AND tf.last_update_time < '"+str(end_time)+"' " \
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
                               "    WHERE session_id = " + str(session_id) + " AND last_update_time >= '"+str(start_time)+"' AND last_update_time < '"+str(end_time)+"' " \
                               "    GROUP BY object_id" \
                               ") mx ON rrp.object_id=mx.object_id and rrp.last_update_time=mx.last_update_time" \
                               " WHERE rrp.session_id = '"+str(session_id)+"' AND rrp.last_update_time >= '"+str(start_time)+"' AND rrp.last_update_time < '"+str(end_time)+"' " \
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
                                "    WHERE session_id = '" + str(session_id) + "' AND last_update_time >= '"+str(start_time)+"' AND last_update_time < '"+str(end_time)+"' " \
                                "    GROUP BY object_id " \
                                ") mx ON aa.object_id=mx.object_id and aa.last_update_time=mx.last_update_time " \
                                " WHERE aa.session_id = '" + str(session_id) + "' " \
                                 " AND aa.last_update_time >= '"+str(start_time)+"' AND aa.last_update_time < '"+str(end_time)+"' " \
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
                            # "is_visible": is_visible
                            }

                result["track_play"][str(t)]["area_alert"].append(aa_track)
            conn.commit()

        print(session_id, "Finished generated")
        track.append(result)
        check_stored = "SELECT * FROM stored_replay WHERE session_id="+str(session_id)+" AND update_rate="+str(UPDATE_RATE)+" "
        cur.execute(check_stored)
        q_store_replay = "INSERT INTO stored_replay(update_rate, session_id, data)" \
                            "	VALUES ("+str(UPDATE_RATE)+", "+str(session_id)+", '"+str(json.dumps(result))+"' )"
        data = cur.fetchall()
        if len(data) == 0:
            cur.execute(q_store_replay)
            conn.commit()
        done_generate.append(session_id)
        print(session_id, "Done")
        # print(q_store_replay)
    # print(json.dumps(result))
    replay_data_send.append(result)
    # print(replay_data_send)

    # print(json.dumps(replay_data_send, default=str))

    conn.commit()


    if USERS:
        message = json.dumps(replay_data_send, default=str)
        await asyncio.wait([user.send(message) for user in USERS])

async def send_reply_data():
    if USERS:
        message = json.dumps(replay_data_send, default=str)
        await asyncio.wait([user.send(message) for user in USERS])

async def register(websocket):
    USERS.add(websocket)
    print(USERS)

async def unregister(websocket):
    USERS.remove(websocket)
async def handler(websocket, path):
    await register(websocket),
    try:
        await send_reply_data()
        async for message in websocket:
            pass
    except websockets.exceptions.ConnectionClosedError:
        print("connection error")
    finally:
        await unregister(websocket)

# start_server = websockets.serve(handler, "10.20.112.217", 8080)
# start_server = websockets.serve(handler, "192.168.43.14", 14045)
start_server = websockets.serve(handler, "127.0.0.1", 8082)

tasks = [
    asyncio.ensure_future(start_server),
    asyncio.ensure_future(get_replay())
]

asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks))
asyncio.get_event_loop().run_forever()
