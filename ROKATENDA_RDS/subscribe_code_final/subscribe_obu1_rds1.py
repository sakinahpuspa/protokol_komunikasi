# subscribe
import random
from paho.mqtt import client as mqtt_client
import sched
import time
from datetime import datetime, timedelta
import os.path
import os
import sys

#RDS 2 Server 2
broker = '202.46.10.7'
port = 1883
topic = "ROKATENDA/RAW/OBU1/RDS"
client_id = f'python-mqtt-{random.randint(0, 1000)}'
username = 'rdsPTIK'
password = 'MosquittoPTIK'

def connect_mqtt(is_subscribing) -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
            if is_subscribing:
                subscribe(client)
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        newpath = str((datetime.utcnow()).strftime("%Y%m%d")) + "/"

        if not os.path.exists("RDS1/" + "OBU1/" + newpath):
            os.makedirs("RDS1/" + "OBU1/" + newpath)
            print("make folder success RDS1/OBU1/" + newpath)
        else:
            print("folder RDS1/OBU1/" + newpath + " created")

        # epochtime
        t = str((datetime.utcnow()).strftime("%Y%m%d%H%M%S.%f")[:-3])
        d = datetime.strptime(t, "%Y%m%d%H%M%S.%f")
        epoch = datetime.utcfromtimestamp(0)

        def unix_time_millis(dt):
            return (dt - epoch).total_seconds() * 1000

        waktu = str(int(unix_time_millis(d)))

        data = msg.payload.decode("utf-8")
        print(data)
        datax = str(data)
        datas = datax.split(" ")
        if len(datas) != 0:
            try:
                if datas[1] == '10':
                    with open(os.path.join("RDS1/OBU1/" + newpath, "bpr_obu1.txt"), 'a+') as b:
                        # b.write("---------------jam sekarang = " + waktu + "---------------" + "\r\n" + data)
                        b.write(data)
                    # print("---------------jam sekarang = " + waktu + "---------------" + "\r\n" + "bpr = " + data)
                    print(data)
                elif datas[1] == '11':
                    with open(os.path.join("RDS1/OBU1/" + newpath, "acc_obu1.txt"), 'a+') as b:
                        # b.write("---------------jam sekarang = " + waktu + "---------------" + "\r\n" + data)
                        b.write(data)
                    # print("---------------jam sekarang = " + waktu + "---------------" + "\r\n" + "acc = " + data)
                    print(data)
                elif datas[1] == '12':
                    with open(os.path.join("RDS1/OBU1/" + newpath, "inklino_obu1.txt"), 'a+') as b:
                        # b.write("---------------jam sekarang = " + waktu + "---------------" + "\r\n" + data)
                        b.write(data)
                    # print("---------------jam sekarang = " + waktu + "---------------" + "\r\n" + "inklino = " + data)
                    print(data)
                elif datas[1] == '14':
                    with open(os.path.join("RDS1/OBU1/" + newpath, "enviro_obu1.txt"), 'a+') as b:
                        # b.write("---------------jam sekarang = " + waktu + "---------------" + "\r\n" + data)
                        b.write(data)
                    # print("---------------jam sekarang = " + waktu + "---------------" + "\r\n" + "enviro = " + data)
                    print(data)
            except:
                pass

    client.subscribe(topic, qos=0)
    client.on_message = on_message


def run():
    if not os.path.exists("RDS1/"):
        os.makedirs("RDS1/")
        print("make folder success RDS1/")
    else:
        print("folder RDS1/ created")
    if not os.path.exists("RDS1/" + "OBU1/"):
        os.makedirs("RDS1/" + "OBU1/")
        print("make folder success RDS1/OBU1/")
    else:
        print("folder RDS1/OBU1/ created")

    client = connect_mqtt(is_subscribing=True)
    client.loop_forever()


if __name__ == '__main__':
    run()

