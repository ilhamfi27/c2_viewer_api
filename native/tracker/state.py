import time, psycopg2
from tracker.config import getconn
from datetime import datetime

conn = getconn()
cur = conn.cursor()

# variable penyimpanan realtime user
USERS = set()

# variable penyimpanan non realtime user
NON_REALTIME_USERS = set()

# application state
DATA_READY = False
FINISHED_CHECK = False

current_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
try:
    start_time_session = "SELECT " \
                         "   start_time " \
                         "FROM sessions " \
                         "WHERE end_time IS NULL;"
    cur.execute(start_time_session)
    start_time = cur.fetchall()
except psycopg2.Error as e:
    print(e)

CREATED_TIME_TRACKS = {
    "replay_system_track_general": start_time[0][0] if len(start_time) > 0 else current_time,
    "replay_system_track_kinetic": start_time[0][0] if len(start_time) > 0 else current_time,
    "replay_system_track_processing": start_time[0][0] if len(start_time) > 0 else current_time,
    "replay_system_track_identification": start_time[0][0] if len(start_time) > 0 else current_time,
    "replay_system_track_link": start_time[0][0] if len(start_time) > 0 else current_time,
    "replay_system_track_mission": start_time[0][0] if len(start_time) > 0 else current_time,
    "replay_track_general_setting": start_time[0][0] if len(start_time) > 0 else current_time,
    "replay_ais_data": start_time[0][0] if len(start_time) > 0 else current_time,
}

ACTIVE_SESSION = None
