import psycopg2
import time
from connection import getconn 

conn = getconn()
cur = conn.cursor()

# query session yang masih end_time = null
q = "SELECT max(id) FROM sessions WHERE end_time IS NULL"
cur.execute(q)
data = cur.fetchall()
for row in data:
    id = row[0]
session_id = 1
#query dari sesi sebelumnya dan masukkan ke 8 tabel dengan id yang sama
q = "SELECT session_id, object_id FROM public.area_alerts WHERE session_id  = "+str(session_id)
q = q+" GROUP BY session_id, object_id ORDER BY 1,2"
cur.execute(q)
data = cur.fetchall()
print(q)
for row in data:
        q = "INSERT INTO public.area_alerts"
        q = q + " SELECT "+ str(id) +", object_type, object_id, warning_type, track_name, last_update_time, mmsi_number, ship_name, track_source_type, is_visible"
        q = q+" FROM public.area_alerts WHERE session_id = " + str(session_id) + " AND object_id = "+str(row[1])
        print(q)
        cur.execute(q)
        conn.commit()
        time.sleep(5)
conn.commit()
cur.close()