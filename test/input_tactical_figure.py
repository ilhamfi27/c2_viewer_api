import pandas as pd
import psycopg2
import time
from connection import getconn 

conn = getconn()
cur = conn.cursor()

df = pd.read_excel('./test_data.xlsx', sheet_name='tactical_figure')
df = df.where(df.notnull(), None)
for index, row in df.iterrows():
    data = []
    data.append(row['object_id'])
    data.append(row['object_type'])
    data.append(row['name'])
    data.append(row['environment'])
    data.append(row['shape'])
    data.append(row['displaying_popup_alert_status'])
    data.append(row['line_color'])
    data.append(row['fill_color'])
    data.append(row['identity_list'])
    data.append(row['warning_list'])
    data.append(row['evaluation_type'])
    data.append(row['visibility_type'])
    data.append(row['last_update_time'])
    data.append(row['network_track_number'])
    data.append(row['link_status_type'])
    data.append(row['is_editable'])
    data.append(row['point_amplification_type'])
    data.append(row['point_keys'])
    data.append(row['points'])

    q = """
    INSERT INTO public.tactical_figure_list(
        object_id, object_type, name, environment, shape, displaying_popup_alert_status, line_color, fill_color, identity_list, warning_list, evaluation_type, visibility_type, last_update_time, network_track_number, link_status_type, is_editable, point_amplification_type, point_keys, points)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    print(data)
    cur.execute(q, data)
    conn.commit()
    time.sleep(5)

conn.commit()
cur.close()