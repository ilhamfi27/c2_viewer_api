import pandas as pd
import time
from base.db_connection import getconn

conn = getconn()
cur = conn.cursor()

df = pd.read_excel('./test_data.xlsx', sheet_name='reference_point')
df = df.where(df.notnull(), None)
for i in range(10):
    for index, row in df.iterrows():
        data = []
        data.append(row['object_type'])
        data.append(row['object_id'] + (116 * i))
        data.append(row['name'] + " iterated by " + str(i) + "")
        data.append(row['latitude'] + (0.5 * i))
        data.append(row['longitude'])
        data.append(row['altitude'])
        data.append(row['visibility_type'])
        data.append(row['point_amplification_type'])
        data.append(row['is_editable'])
        data.append(row['network_track_number'])
        data.append(row['link_status_type'])
        data.append(row['last_update_time'])

        q = """
        INSERT INTO public.reference_points(
            object_type, object_id, name, latitude, longitude, altitude, visibility_type, point_amplification_type, is_editable, network_track_number, link_status_type, last_update_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        print(data)
        cur.execute(q, data)
        conn.commit()
        time.sleep(5)

conn.commit()
cur.close()