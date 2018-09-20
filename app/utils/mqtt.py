import paho.mqtt.client as mqtt
import time

def create_mqtt(ip, port, username, password):
    def on_message(mqttc, obj, msg):
        print(mqttc, obj, msg.payload)

    mc = mqtt.Client(client_id=str(time.time()))
    mc.on_message = on_message
    mc.username_pw_set(username, password)
    mc.connect(ip, port)
    mc.loop_start()
    return mc