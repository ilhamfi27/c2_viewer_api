from main import *

USERS = set()
dots    = {}
async def history_dot(system_track_number):
    history_dot_query = "Select max(latitude) as lat,max(longitude) as long,last_update_time    \
                    from replay_system_track_kinetic \
                    where system_track_number="+str(system_track_number)+" \
                    group by last_update_time \
                    order by last_update_time asc"
    
    cur.execute(history_dot_query)
    data    = cur.fetchall()
    
    i       = 0
    for dot in data:
        latitude    = dot[0]
        longitude   = dot[1]
        time        = dot[2]
        dots[i]     = {"latitude":latitude, "longitude": longitude, "time":time}
        i = i+1
    print(dots)
    if USERS:
        message = json.dumps(json.dumps(dots), default=str)
        await asyncio.wait([user.send(message) for user in USERS])        

async def register(websocket):
    USERS.add(websocket)
    print(USERS)

async def send_reply_data():
    if USERS:
        message = json.dumps(dots, default=str)
        await asyncio.wait([user.send(message) for user in USERS])

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

start_server = websockets.serve(handler, "127.0.0.1", 8082)

tasks = [
    asyncio.ensure_future(start_server),
    asyncio.ensure_future(history_dot(1))
]

asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks))
asyncio.get_event_loop().run_forever()

