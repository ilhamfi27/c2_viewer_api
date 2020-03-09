#!/usr/bin/env python

# WS server example that synchronizes REALTIME_state across clients


from main import *
from replay_generator_alternative import *
import time, threading

def check_replay():    
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
    threading.Timer(1, check_replay).start()
if __name__ == "__main__":    
    check_replay()
    




