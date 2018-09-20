import motor.motor_asyncio
import asyncio

db = motor.motor_asyncio.AsyncIOMotorClient('localhost', 27017)['test1']


@asyncio.coroutine
def do_find():
    cursor = db['card'].find({"name": "abc"})
    count = 0

    for doc in (yield from cursor.to_list(length=10)):
        print(doc)




loop = asyncio.get_event_loop()
loop.run_until_complete(do_find())
