#!/usr/bin/env python

# WS server example that synchronizes REALTIME_state across clients


from main import *
from replay_generator import *

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




