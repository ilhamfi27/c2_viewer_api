#!/home/patrick/anaconda3/bin/python 

from main import *
from replay_fetchone import *
import datetime

now = datetime.datetime.now()
# f = open(now.strftime("%Y-%m-%d %H:%M:%S")+".txt", "w+")

if r.exists("is_generating"):
    print(r.get("is_generating"))
else:
    r.set("is_generating", "0")

if r.get("is_generating").decode("utf-8") == "0":    
    get_replay()    
else:    
    print("Sedang menggenerate replay")




