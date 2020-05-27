#!/usr/bin/env python3

# WS server example that synchronizes REALTIME_state across clients
from main import *
from replay_refined import *

if r.exists("is_generating"):
    print(r.get("is_generating"))
else:
    r.set("is_generating", "0")

if r.get("is_generating").decode("utf-8") == "0":    
    get_replay()    
else:    
    print("Sedang menggenerate replay")


