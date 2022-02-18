###################################################################################################
#library
###################################################################################################
from twisted.internet import tksupport, reactor
from twisted.protocols.policies import TimeoutMixin
from twisted.application.internet import ClientService
from twisted.internet.protocol import ReconnectingClientFactory as ClFactory
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.internet import reactor, protocol
from twisted.internet.protocol import Protocol
from collections import deque
from queue import Queue
import sched, time
from datetime import datetime, timedelta
from paho.mqtt import client as mqtt_client
import os.path
import os
import sys
import yaml
from filelog import setup_logger, logfiledata
###################################################################################################
#CONFIG
###################################################################################################
with open("config.yaml", "r") as ymlfile:
    cfg = yaml.safe_load(ymlfile)

newpath = str((datetime.utcnow()).strftime("%Y%m")) + "/"
tanggal = str((datetime.utcnow()).strftime("%Y%m%d"))

# queue dari BPR untuk dijadikan input Proses TDA
queueACL_OBU1 = Queue()
queueACL_OBU2 = Queue()
queueBPR_OBU1 = Queue()
queueBPR_OBU2 = Queue()
queue_mqttacc_obu1 = Queue()
queue_mqttbpr_obu1 = Queue()
queue_mqttacc_obu2 = Queue()
queue_mqttbpr_obu2 = Queue()

###################################################################################################
# ACC and BPR Data Processing Start
###################################################################################################
#class prosesData():
def parsingACC(payload):
    parsing_logger.info("Process parsing acc start")
    datax = str(payload)
    data = datax.split(cfg["other"]["delimiter"])
    parsing_logger.info("Process parsing acc done")
    return data

def parsingBPR(payload):
    parsing_logger.info("Process parsing bpr start")
    datax = str(payload)
    data = datax.split(cfg["other"]["delimiter"])
    parsing_logger.info("Process parsing bpr done")
    return data

def prosesACC(tipe_obu, queueACC, queue_mqtt_acc):
    print(cfg["TIPEOBU"]["SENSOR"][4] + '\n')
    lastTS = 0
    count = 0
    datafinal = ""
    while True:
        try:
            dataInput = queueACC.get()
            data_raw = str((dataInput['ts']).strftime("%d %m %Y %H %M %S.%f")[:-3]) + " " + str(dataInput['data'])
            logfiledata(newpath, cfg["path"]["raw_log"], tipe_obu, cfg["TIPEOBU"]["SENSOR"][4], tanggal, data_raw, cfg["path"]["fileraw_txt"])

            akuisisi_logger.info("Process data accelerometer per detik")
            if int((dataInput['ts']).strftime("%S")) != lastTS:
                lastTS = int((dataInput['ts']).strftime("%S"))
                logfiledata(newpath, cfg["path"]["final_log"], tipe_obu, cfg["TIPEOBU"]["SENSOR"][4], tanggal, datafinal, cfg["path"]["file_txt"])
                #print(datafinal)
                logfiledata(newpath, cfg["path"]["final_log"], tipe_obu, cfg["TIPEOBU"]["SENSOR"][4], tanggal, str(count) + '\r\n', cfg["path"]["file_txt"])
                #print('Data Accelerometer ' + tipe_obu + ' = ' + str(count))
                queue_mqtt_acc.put(datafinal)
                count = 0
                datafinal = ""

            for isi in dataInput['data'].split('\r\n'):
                dataacc = parsingACC(isi)
                if len(dataacc) == 4:
                    akuisisi_logger.info("Process pembentukan format data")

                    datafinal += tipe_obu + " " + cfg["TIPEOBU"]["TIPESENSOR"][1] + " " + str(
                        (dataInput['ts']).strftime("%d %m %Y %H %M %S.%f")[:-3]) + ' ' + dataacc[0] + ' ' + dataacc[
                                     1] + ' ' + dataacc[2] + '\r\n'
                    count += 1

        except:
            pass
        """except KeyboardInterrupt:
            print('Interrupted')
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)"""

def prosesBPR(tipe_obu, queueBPR, queue_mqtt_bpr):
    print(cfg["TIPEOBU"]["SENSOR"][5] + '\n')
    while True:
        try:
            dataInput = queueBPR.get()
            data_raw = str((dataInput['ts']).strftime("%d %m %Y %H %M %S.%f")[:-3]) + " " + str(dataInput['data'])
            logfiledata(newpath, cfg["path"]["raw_log"], tipe_obu, cfg["TIPEOBU"]["SENSOR"][5], tanggal, data_raw, cfg["path"]["fileraw_txt"])

            databpr = parsingBPR(dataInput['data'])

            akuisisi_logger.info("Process pembentukan format data")
            data_final = str(tipe_obu) + " " + cfg["TIPEOBU"]["TIPESENSOR"][0] + " " + str(
                (dataInput['ts']).strftime("%d %m %Y %H %M %S.%f")[:-3]) + ' ' + databpr[1] + ' ' + databpr[2] + '\r\n'
            #print(data_final)
            logfiledata(newpath, cfg["path"]["final_log"], tipe_obu, cfg["TIPEOBU"]["SENSOR"][5], tanggal, data_final, cfg["path"]["file_txt"])

            queue_mqtt_bpr.put(data_final)
            akuisisi_logger.info("send data final to queue mqtt")

        except:
            pass

###################################################################################################
# ACC and BPR Data Processing End
###################################################################################################
###################################################################################################
# TCP Client Start
###################################################################################################

class Client(protocol.Protocol):
    closeCount = 0

    def __init__(self, stopTrying=False):
        self.stopTrying = stopTrying
        self.Dictdata = {"ts": 0, "data": ""}

    def connectionMade(self):
        print("Connection Made")
        akuisisi_logger.info("Connection Made")

        reactor.callLater(3.5, self.initBPR, "*0100E4\r\n")
        akuisisi_logger.info("send command '*0100E4' to sensor BPR")


    def dataReceived(self, data):
        self.closeCount += 1
        self.Dictdata['ts'] = datetime.utcnow()
        self.Dictdata['data'] = data.decode()
        self.insertToQueue(self.Dictdata)


    def initBPR(self, message):
        self.transport.write(message.encode())

    def timeoutConnection(self):
        print("timeout")
        akuisisi_logger.info("timeout")

    def insertToQueue(self, data):
        pass


class client1(Client):
    def insertToQueue(self, data):
        queueACL_OBU1.put(data)
        akuisisi_logger.info("get data sensor accelerometer obu1")


class client2(Client):
    def insertToQueue(self, data):
        queueBPR_OBU1.put(data)
        akuisisi_logger.info("get data sensor bpr obu1")


class client3(Client):
    def insertToQueue(self, data):
        queueACL_OBU2.put(data)
        akuisisi_logger.info("get data sensor accelerometer obu2")


class client4(Client):
    def insertToQueue(self, data):
        queueBPR_OBU2.put(data)
        akuisisi_logger.info("get data sensor bpr obu2")


class ClientFactory(ClFactory):
    def __init__(self, source):
        self.source = source
        self.maxDelay = 1
        self.stopTrying = False

    def startedConnecting(self, connector):
        print('Started to connect.')
        akuisisi_logger.info("Started to connect.")

    def buildProtocol(self, addr):
        print('Connected.')
        akuisisi_logger.info('Connected.')

        print('Resetting reconnection delay')
        akuisisi_logger.info('Resetting reconnection delay')

        self.resetDelay()
        if (self.source == 1):
            protocol = client1
        elif (self.source == 2):
            protocol = client2
        elif (self.source == 3):
            protocol = client3
        elif (self.source == 4):
            protocol = client4

        return protocol(self.stopTrying)

    def clientConnectionLost(self, connector, reason):
        print('Connection lost from IP : {} Port : {}\nReason:{}'.format(connector.getDestination().host,
                                                                         connector.getDestination().port, reason))
        akuisisi_logger.error('Connection lost from IP : {} Port : {}\nReason:{}'.format(connector.getDestination().host,
                                                                         connector.getDestination().port, reason))

        ClFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        print('Connection failed from IP : {} Port : {}\nReason:{}'.format(connector.getDestination().host,
                                                                           connector.getDestination().port, reason))
        akuisisi_logger.error('Connection failed from IP : {} Port : {}\nReason:{}'.format(connector.getDestination().host,
                                                                           connector.getDestination().port, reason))

        ClFactory.clientConnectionFailed(self, connector, reason)
###################################################################################################
# TCP Client Stop
###################################################################################################
###################################################################################################
#connect and publish mqtt start
###################################################################################################
#class mqtt():
def connect_mqtt(tipe):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            #print("Connected to MQTT Broker!")
            mqtt_logger.info("Connected to MQTT Broker!")
        else:
            #print("Failed to connect, return code %d\n", rc)
            mqtt_logger.error("Failed to connect, return code")

    def on_publish(client, userdata, mid):
        print(str(mid) + " = " + tipe + " " + "published")

    client = mqtt_client.Client(cfg["mqtt"]["client_id"])
    client.username_pw_set(cfg["mqtt"]["username"], cfg["mqtt"]["password"])
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.connect(cfg["mqtt"]["broker"], cfg["mqtt"]["port"])
    return client


def sendData(topic_obu, tipe, queue_mqtt):
    msg_count = 0
    client = connect_mqtt(tipe)
    client.loop_start()

    while True:
        try:
            if queue_mqtt.empty() == False:
                mqtt_logger.info("data available in queue mqtt")
                data_to_send = queue_mqtt.get()
                result = client.publish(cfg["mqtt"]["topic"]+topic_obu, data_to_send, qos=cfg["mqtt"]["qoslevel"])
                status = result[0]
                if status == 0:
                    #print("success")
                    mqtt_logger.info("Send data success to topic")
                else:
                    #print("failed")
                    mqtt_logger.error("Failed to send data to topic")
                msg_count += 1

            else:
                time.sleep(1)
                mqtt_logger.info("no data to publish")

        except:
            pass


###################################################################################################
#connect and publish mqtt end
###################################################################################################
###################################################################################################
# Main function
###################################################################################################
connectorACL = []
connectorBPR = []
i = 0

if not os.path.exists(newpath):
    os.makedirs(newpath)
    os.makedirs(newpath + cfg["path"]["raw_log"])
    os.makedirs(newpath + cfg["path"]["final_log"])
    os.makedirs(newpath + cfg["path"]["folder_Log"])
    print("make folder success")
else:
    print("folder created")

akuisisi_logger = setup_logger('akuisisi_logger', newpath + cfg["path"]["folder_Log"] + cfg["TIPEOBU"]["LOKASI"][2] + cfg["path"]["akuisisi_log"] + tanggal + '.log')
parsing_logger = setup_logger('parsing_logger', newpath + cfg["path"]["folder_Log"] + cfg["TIPEOBU"]["LOKASI"][2] + cfg["path"]["parsing_log"] + tanggal + '.log')
mqtt_logger = setup_logger('mqtt_logger', newpath + cfg["path"]["folder_Log"] + cfg["TIPEOBU"]["LOKASI"][2] + cfg["path"]["mqtt_log"] + tanggal + '.log')

akuisisi_logger.info("Start Process")

for n in range(0, 2):
    connectorACL.append(reactor.connectTCP(cfg["obuAddressList"]["IP"][i], cfg["obuAddressList"]["PORT"][i], ClientFactory(1 + i),
                                           bindAddress=(cfg["lsAddressList"]["IP"][i], cfg["lsAddressList"]["PORT"][i])))
    connectorBPR.append(reactor.connectTCP(cfg["obuAddressList"]["IP"][i + 1], cfg["obuAddressList"]["PORT"][i + 1], ClientFactory(2 + i),
                                           bindAddress=(cfg["lsAddressList"]["IP"][i + 1], cfg["lsAddressList"]["PORT"][i + 1])))
    i += 2
reactor.callInThread(prosesACC, cfg["TIPEOBU"]["LOKASI"][0], queueACL_OBU1, queue_mqttacc_obu1)
reactor.callInThread(prosesBPR, cfg["TIPEOBU"]["LOKASI"][0], queueBPR_OBU1, queue_mqttbpr_obu1)
reactor.callInThread(prosesACC, cfg["TIPEOBU"]["LOKASI"][1], queueACL_OBU2, queue_mqttacc_obu2)
reactor.callInThread(prosesBPR, cfg["TIPEOBU"]["LOKASI"][1], queueBPR_OBU2, queue_mqttbpr_obu2)
reactor.callInThread(sendData, cfg["TIPEOBU"]["OBU"][0], cfg["TIPEOBU"]["SENSOR"][0], queue_mqttacc_obu1)
reactor.callInThread(sendData, cfg["TIPEOBU"]["OBU"][0], cfg["TIPEOBU"]["SENSOR"][1], queue_mqttbpr_obu1)
reactor.callInThread(sendData, cfg["TIPEOBU"]["OBU"][1], cfg["TIPEOBU"]["SENSOR"][2], queue_mqttacc_obu2)
reactor.callInThread(sendData, cfg["TIPEOBU"]["OBU"][1], cfg["TIPEOBU"]["SENSOR"][3], queue_mqttbpr_obu2)
reactor.run()