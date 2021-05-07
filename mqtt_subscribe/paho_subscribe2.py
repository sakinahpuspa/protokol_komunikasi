#subscribe
import random
from paho.mqtt import client as mqtt_client
import sched, time
from datetime import datetime, timedelta
import os.path
import os
import sys


broker = '202.46.8.100'
port = 1883
topic = "test/CBT/DATA/OBU2"
client_id = f'python-mqtt-{random.randint(0, 100)}'
username = 'rdsPTIK'
password = 'MosquittoPTIK'
newpath = str((datetime.utcnow()).strftime("%Y%m")) + "/"


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        data = msg.payload.decode()
        print(f"{data}")
        with open(os.path.join("RDS/" + "OBU2/" + newpath, "data_obu2.txt"), 'a') as b:
            b.write(data)
    client.subscribe(topic, qos=2)
    client.on_message = on_message


def run():
    if not os.path.exists("RDS/"):
        os.makedirs("RDS/")
        print("make folder success RDS/")
    else:
        print("folder RDS/ created")
    if not os.path.exists("RDS/" + "OBU2/"):
        os.makedirs("RDS/" + "OBU2/")
        print("make folder success RDS/OBU2/")
    else:
        print("folder RDS/OBU2/ created")
    if not os.path.exists("RDS/" + "OBU2/" + newpath):
        os.makedirs("RDS/" + "OBU2/" + newpath)
        print("make folder success RDS/OBU2/" + newpath)
    else:
        print("folder RDS/OBU2/" + newpath + " created")


    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()

