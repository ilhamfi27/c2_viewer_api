import psycopg2
import time
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.config import getconn

conn = getconn()
cur = conn.cursor()
ar_mandatory_table_8 = ['replay_system_track_general','replay_system_track_kinetic','replay_system_track_processing','replay_system_track_identification','replay_system_track_link','replay_system_track_mission','replay_track_general_setting','replay_ais_data']
#query session yang masih end_time = null
q = "SELECT max(id) FROM sessions WHERE end_time IS NULL"
cur.execute(q)
data = cur.fetchall()
for row in data:
    id = row[0]
    q1 = "UPDATE sessions set end_time = CURRENT_TIMESTAMP WHERE id = "+str(id)
    cur.execute(q1)
    conn.commit()
#buat sesi baru
print(id)
id_baru = id+1
session_id = 1
print(id_baru)
q1 = "INSERT INTO sessions(id,name,start_time) VALUES("+str(id_baru)+",'Rec Session "+str(id_baru)+"',CURRENT_TIMESTAMP)"
cur.execute(q1)
conn.commit()
#query dari sesi sebelumnya dan masukkan ke 8 tabel dengan id yang sama
q = "SELECT session_id,system_track_number FROM public.replay_system_track_general WHERE session_id  = "+str(session_id)
q = q+" GROUP BY session_id,system_track_number ORDER BY 1,2"
cur.execute(q)
data = cur.fetchall()
print(q)
for x in range(12):
    for row in data:
        for i in range(len(ar_mandatory_table_8)):
            q = "INSERT INTO "+ar_mandatory_table_8[i]
            if(ar_mandatory_table_8[i]=='replay_system_track_general'):
                q = q+" SELECT "+str(id_baru)+"," + \
                    "system_track_number + (SELECT MAX(system_track_number) FROM "+ar_mandatory_table_8[i]+" WHERE session_id = "+str(row[0])+") * "+str(x)+"," + \
                    " track_name, network_track_number, identity, environment, object_type, object_id, primitive_data_source, accuracy_level, source, own_unit_indicator, iu_indicator, c2_indicator, special_processing_indicator, force_tell_indicator, emergency_indicator, simulation_indicator, last_update_time, initiation_time, CURRENT_TIMESTAMP as created_time, airborne_indicator"
            elif(ar_mandatory_table_8[i]=='replay_system_track_kinetic'):
                q = q+" SELECT "+str(id_baru)+"," + \
                    "system_track_number + (SELECT MAX(system_track_number) FROM "+ar_mandatory_table_8[i]+" WHERE session_id = "+str(row[0])+") * "+str(x)+"," + \
                    " track_name, heading, latitude  + ("+ str(i * 0.1) +"), longitude, range, bearing, height_depth, speed_over_ground, course_over_ground, CURRENT_TIMESTAMP as last_update_time, CURRENT_TIMESTAMP as created_time"
            elif(ar_mandatory_table_8[i]=='replay_system_track_processing'):
                q = q+" SELECT "+str(id_baru)+"," + \
                    "system_track_number + (SELECT MAX(system_track_number) FROM "+ar_mandatory_table_8[i]+" WHERE session_id = "+str(row[0])+") * "+str(x)+"," + \
                    " track_fusion_status, track_join_status, daughter_tracks, track_phase_type, track_suspect_level, CURRENT_TIMESTAMP as created_time"
            elif(ar_mandatory_table_8[i]=='replay_system_track_identification'):
                q = q+" SELECT "+str(id_baru)+"," + \
                    "system_track_number + (SELECT MAX(system_track_number) FROM "+ar_mandatory_table_8[i]+" WHERE session_id = "+str(row[0])+") * "+str(x)+"," + \
                    " identity, environment, air_platform, surf_platform, land_platform, air_platform_activity, surf_platform_activity, land_platform_activity, air_specific, surf_specific, land_specific, CURRENT_TIMESTAMP as created_time"
            elif(ar_mandatory_table_8[i]=='replay_system_track_link'):
                q = q+" SELECT "+str(id_baru)+"," + \
                    "system_track_number + (SELECT MAX(system_track_number) FROM "+ar_mandatory_table_8[i]+" WHERE session_id = "+str(row[0])+") * "+str(x)+"," + \
                    " network_track_number, associated_track_number, originator_address, controlling_unit_address, network_track_quality, link_status, CURRENT_TIMESTAMP as created_time"
            elif(ar_mandatory_table_8[i]=='replay_system_track_mission'):
                q = q+" SELECT "+str(id_baru)+"," + \
                    "system_track_number + (SELECT MAX(system_track_number) FROM "+ar_mandatory_table_8[i]+" WHERE session_id = "+str(row[0])+") * "+str(x)+"," + \
                    " mission_name, route, voice_call_sign, voice_frequency_channel, fuel_status, radar_coverage, start_time, end_time, CURRENT_TIMESTAMP as created_time"
            elif(ar_mandatory_table_8[i]=='replay_track_general_setting'):
                q = q+" SELECT "+str(id_baru)+"," + \
                    "system_track_number + (SELECT MAX(system_track_number) FROM "+ar_mandatory_table_8[i]+" WHERE session_id = "+str(row[0])+") * "+str(x)+"," + \
                    " speed_label_visibility, track_name_label_visibility, radar_coverage_visibility, track_visibility, CURRENT_TIMESTAMP as created_time"
            elif(ar_mandatory_table_8[i]=='replay_ais_data'):
                q = q+" SELECT "+str(id_baru)+"," + \
                    "system_track_number + (SELECT MAX(system_track_number) FROM "+ar_mandatory_table_8[i]+" WHERE session_id = "+str(row[0])+") * "+str(x)+"," + \
                    " mmsi_number, name, radio_call_sign, imo_number, navigation_status, destination, dimensions_of_ship, type_of_ship_or_cargo, rate_of_turn, position_accuracy, gross_tonnage, ship_country, CURRENT_TIMESTAMP as created_time, eta_at_destination, vendor_id"
            else:
                q = q
            q = q+" FROM "+ar_mandatory_table_8[i]+" WHERE session_id = "+str(row[0])+" AND system_track_number = "+str(row[1]) + " LIMIT 1"
            cur.execute(q)
            conn.commit()
            print(i,ar_mandatory_table_8[i],q)
            time.sleep(1)
            
        # gerakin data2
        q_select = "SELECT session_id,system_track_number FROM public.replay_system_track_kinetic WHERE session_id  = "+str(id_baru)
        q_select = q_select+" GROUP BY session_id,system_track_number ORDER BY 1,2"
        cur.execute(q_select)
        data_gerak = cur.fetchall()
        for gerak in data_gerak:
            qd_gerak = "INSERT INTO replay_system_track_kinetic"
            qd_gerak = qd_gerak+" SELECT "+str(gerak[0])+"," + \
                        "system_track_number, track_name, heading, aa.latitude  + ("+ str(0.4) +"), longitude, range, bearing, height_depth, speed_over_ground, course_over_ground, CURRENT_TIMESTAMP as last_update_time, CURRENT_TIMESTAMP as created_time"
            qd_gerak = qd_gerak + " FROM (SELECT * FROM replay_system_track_kinetic WHERE system_track_number = "+str(gerak[1])+" ORDER BY created_time DESC LIMIT 1) aa "
            qd_gerak = qd_gerak + " WHERE session_id = "+str(gerak[0])+" AND system_track_number = "+str(gerak[1])
            qd_gerak = qd_gerak + " ORDER BY aa.created_time DESC LIMIT 1"
            cur.execute(qd_gerak)
            conn.commit()
            print("YANG GERAK *********|||||||||***********", qd_gerak)
cur.close()