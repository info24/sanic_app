import asyncio
import socket
import aiohttp
from asyncio import test_utils
from sanic import Sanic
from sanic.response import json
from sanic import Blueprint

import config
from app.utils.mqtt import create_mqtt
from app.utils.qiniu import Qiniu


# app = Sanic()
client_blueprint = Blueprint("client_blueprint", url_prefix="/client")


qiniu = Qiniu(config.BUCKET_NAME, config.QINIU_URL, config.QINIU_ACS, config.QINIU_SEC)


# import motor.motor_asyncio
# client = motor.motor_asyncio.AsyncIOMotorClient('localhost', 27017)

loop = asyncio.get_event_loop()
from functools import partial

client = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
client.bind(("0.0.0.0", 9003))


@asyncio.coroutine
async def foo():
    async with aiohttp.ClientSession() as session:
        async with session.get("http://www.baidu.com") as r:
            bb = await r.text()
            print("bb")
    # a =  yield from asyncio.sleep(5)
    # print(a)

@asyncio.coroutine
async def callback_open(data, url):
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=data) as r:
            bb = await r.text()
            print(bb)

def publish(mqtt_client, data):
    print(data)
    data = data.decode()
    lists = data.split(",")
    url = qiniu.get_url(lists[0])
    mqtt_client.publish("zghl_door/door_open", "%s, %s, %s, %s, %s, %s, %s"%(url, lists[1], lists[-8], lists[-7], lists[-6], lists[-4], lists[-2]), qos=2)

mqtt_client = create_mqtt(config.MQTT_IP, config.MQTT_PORT, config.MQTT_USERNAME, config.MQTT_PASSWORD)

publish_p = partial(publish, mqtt_client)

def reader(client, func):
    try:
        (data, addr) = client.recvfrom(1024)
        print(data, addr, "data")
        func(data)
        # loop.create_task(foo())
    except Exception as e:
        print("error", e)

reader_p = partial(reader, client, publish_p)


@client_blueprint.listener("before_server_start")
def install_udp_server(client_blueprint, loop):
    loop.add_reader(client.fileno(), reader_p)


@client_blueprint.route("/hi")
def test(request):
    '''测试路由
    '''
    return json({"hi": "ok"})

# loop.add_reader(client.fileno(), reader_p)
# test_utils.run_once(loop)
# loop.run_forever()
