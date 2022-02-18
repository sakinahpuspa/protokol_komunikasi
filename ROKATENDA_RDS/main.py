###################################################################################################
# library
# edited by ....
###################################################################################################
from twisted.internet import reactor, task
from twisted.protocols.policies import TimeoutMixin
from twisted.application.internet import ClientService
from twisted.internet.protocol import ReconnectingClientFactory as ClFactory
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.internet import reactor, protocol
from twisted.internet.protocol import Protocol
from collections import deque
from queue import Queue
import sched
import time
from datetime import datetime, timedelta
from paho.mqtt import client as mqtt_client
import os.path
import os
import sys
import yaml
from filelog import setup_logger, logfiledata, delete_logger
from ping3 import ping
import random


###################################################################################################
# CONFIG
###################################################################################################

folder = "C:/Users/USER/Documents/Software_Protokol_Komunikasi/LABUANBAJO_LOCAL/"

with open(folder+"config.yaml", "r") as ymlfile:
    cfg = yaml.safe_load(ymlfile)

client_id = f'python-mqtt-{random.randint(0, 1000)}'

# queue
queueAllBJOOBU = []
queueAllBPROBU = []
queueAllACLMQTT = []
queueAllBPRMQTT = []
queueAllINKLINOMQTT = []
queueAllENVIROMQTT = []
queueMQTT = []

###################################################################################################
# BAJO and BPR Data Processing Start
###################################################################################################
def parsingBJO(payload):
    datax = str(payload)
    datas = datax.split(cfg["other"]["delimiter2"])
    data = datas[0].split(cfg["other"]["delimiter1"])
    return data


def parsingBPR(payload):
    # parsing_logger.info("Process parsing bpr start")
    datax = str(payload)
    data = datax.split(cfg["other"]["delimiter1"])
    # parsing_logger.info("Process parsing bpr done")
    return data


def checksum(dataMasuk):
    dataMasuk.rstrip("\r\n").rstrip("\r")
    hasil = False
    if dataMasuk[0] == '$':
        dataMasuk = dataMasuk[1:]
        buff = dataMasuk.split('*')
        if len(buff) == 2:
            ck_sum = 0
            for kar in buff[0]:
                ck_sum ^= ord(kar)
                if buff[1] != '':
                    if ck_sum == int(buff[1], 16):
                        hasil = True
    return hasil


def dataBJOeror(tipe_obu, noOBU, waktu):
    akuisisi_logger.info("data raw obu " + str(int(noOBU)+1) + " bajo eror")
    data_a = "$01,1,99.000,99.000,99.000,99.000*2E"
    datarawacc = waktu + " " + data_a + '\r\n'
    # parsing_logger.info("Process parsing acc start")
    dataacc = parsingBJO(data_a)
    # parsing_logger.info("Process parsing acc done")
    # akuisisi_logger.info("Process pembentukan format data accelerometer 99.000")
    datafinalacc = tipe_obu + " " + cfg["TIPEOBU"]["TIPESENSOR"][1] + " " + waktu + ' ' + \
                   dataacc[2] + ' ' + dataacc[3] + ' ' + dataacc[4] + ' ' + dataacc[
                       5] + '\r\n'
    dataACC = [datarawacc, datafinalacc]

    return dataACC

def dataBPReror(tipe_obu, noOBU, waktu):
    akuisisi_logger.info("data raw obu " + str(int(noOBU)+1) + " bpr eror")
    data_b = "*0001,9999.000,9999.000"
    datarawbpr = waktu + " " + data_b + '\r\n'
    databpr = parsingBPR(data_b)
    # akuisisi_logger.info("Process pembentukan format data bpr 9999.000")
    datafinalbpr = str(tipe_obu) + " " + cfg["TIPEOBU"]["TIPESENSOR"][0] + " " + waktu + ' ' + databpr[
        1] + ' ' + databpr[2] + '\r\n'
    # print(datafinalbpr)
    logfiledata(folder + newpath, cfg["path"]["raw_log"], 'BPR/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][1], times,
                datarawbpr, cfg["path"]["fileraw_txt"])
    logfiledata(folder + newpath, cfg["path"]["final_log"], 'BPR/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][1],
                times, datafinalbpr, cfg["path"]["file_txt"])
    queueMQTT[noOBU].put(datafinalbpr)
    # akuisisi_logger.info("send data final bpr 9999.000 to queue mqtt")

def prosesBJO(tipe_obu, noOBU):
    # akuisisi_logger.info(cfg["TIPEOBU"]["SENSOR"][5] + '\r\n')
    lastTS = 0
    count = 0
    datafinalacc = ""
    datarawacc = ""
    beforetime = 0
    while True:
        try:
            if connectIP[noOBU] == "down":
                akuisisi_logger.info("tidak ada data karena nport atau obu " + str(int(noOBU)+1) + " mati")
                time.sleep(1)
                # epochtime
                t = str((datetime.utcnow()).strftime("%Y%m%d%H%M%S.%f")[:-3])
                d = datetime.strptime(t, "%Y%m%d%H%M%S.%f")
                epoch = datetime.utcfromtimestamp(0)

                def unix_time_millis(dt):
                    return (dt - epoch).total_seconds() * 1000

                waktu = str(int(unix_time_millis(d)))

                data_a = "$01,1,99.990,99.990,99.990,99.990*2E"
                datarawacc = waktu + " " + data_a + '\r\n'
                # parsing_logger.info("Process parsing acc start")
                dataacc = parsingBJO(data_a)
                # parsing_logger.info("Process parsing acc done")
                # akuisisi_logger.info("Process pembentukan format data accelerometer 99.990")
                datafinalacc = tipe_obu + " " + cfg["TIPEOBU"]["TIPESENSOR"][1] + " " + waktu + ' ' + \
                               dataacc[2] + ' ' + dataacc[3] + ' ' + dataacc[4] + ' ' + dataacc[
                                   5] + '\r\n'
                # print(datafinalacc)
                logfiledata(folder + newpath, cfg["path"]["raw_log"], 'ACC/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][0], times,
                            datarawacc, cfg["path"]["fileraw_txt"])
                logfiledata(folder + newpath, cfg["path"]["final_log"], 'ACC/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][0],
                            times, datafinalacc, cfg["path"]["file_txt"])
                queueMQTT[noOBU].put(datafinalacc)
                # akuisisi_logger.info("send data final accelerometer 99.990 to queue mqtt")

            elif ket_bjo[noOBU] == "no" and connectIP[noOBU] == "up":
                akuisisi_logger.info("tidak ada data raw obu " + str(int(noOBU)+1) + " bajo di queue maupun dari nport")
                time.sleep(1)
                # epochtime
                t = str((datetime.utcnow()).strftime("%Y%m%d%H%M%S.%f")[:-3])
                d = datetime.strptime(t, "%Y%m%d%H%M%S.%f")
                epoch = datetime.utcfromtimestamp(0)

                def unix_time_millis(dt):
                    return (dt - epoch).total_seconds() * 1000

                waktu = str(int(unix_time_millis(d)))

                data_a = "$01,1,99.900,99.900,99.900,99.900*2E"
                datarawacc = waktu + " " + data_a + '\r\n'
                # parsing_logger.info("Process parsing acc start")
                dataacc = parsingBJO(data_a)
                # parsing_logger.info("Process parsing acc done")
                # akuisisi_logger.info("Process pembentukan format data accelerometer 99.900")
                datafinalacc = tipe_obu + " " + cfg["TIPEOBU"]["TIPESENSOR"][1] + " " + waktu + ' ' + \
                               dataacc[2] + ' ' + dataacc[3] + ' ' + dataacc[4] + ' ' + dataacc[
                                   5] + '\r\n'
                # print(datafinalacc)
                logfiledata(folder + newpath, cfg["path"]["raw_log"], 'ACC/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][0], times,
                            datarawacc, cfg["path"]["fileraw_txt"])
                logfiledata(folder + newpath, cfg["path"]["final_log"], 'ACC/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][0],
                            times, datafinalacc, cfg["path"]["file_txt"])
                queueMQTT[noOBU].put(datafinalacc)
                # akuisisi_logger.info("send data final acc 99.900 to queue mqtt")

            elif ket_bjo[noOBU] == "yes" and connectIP[noOBU] == "up":
                try:
                    dataInput = queueAllBJOOBU[noOBU].get(True, 2)

                    # epochtime
                    t = str((dataInput['ts']).strftime("%Y%m%d%H%M%S.%f")[:-3])
                    d = datetime.strptime(t, "%Y%m%d%H%M%S.%f")
                    epoch = datetime.utcfromtimestamp(0)

                    def unix_time_millis(dt):
                        return (dt - epoch).total_seconds() * 1000

                    waktu = str(int(unix_time_millis(d)))
                    if int(waktu) != beforetime:
                        beforetime = int(waktu)
                        if len(str(dataInput['data']).split("\r\n")) == 2:
                            data_a = str(dataInput['data']).split("\r\n")[0]
                            if data_a != '':
                                # akuisisi_logger.info("data raw bajo tidak ''")
                                hasil = checksum(str(data_a))
                                if hasil:
                                    # akuisisi_logger.info("data bajo hasil checksum true")
                                    # parsing_logger.info("Process parsing tipe data bajo start")
                                    tipedata = parsingBJO(str(data_a))[1]
                                    # parsing_logger.info("Process parsing tipe data bajo done")
                                    if tipedata == '1':
                                        # akuisisi_logger.info("data bajo termasuk kategori tipe data 1")
                                        datarawacc += waktu + " " + data_a + '\r\n'
                                        # parsing_logger.info("Process parsing acc start")
                                        dataacc = parsingBJO(data_a)
                                        # parsing_logger.info("Process parsing acc done")
                                        if len(dataacc) == 6:
                                            # akuisisi_logger.info("Process pembentukan format data accelerometer")
                                            datafinalacc += tipe_obu + " " + cfg["TIPEOBU"]["TIPESENSOR"][
                                                1] + " " + waktu + ' ' + \
                                                            dataacc[2] + ' ' + dataacc[3] + ' ' + dataacc[4] + ' ' + \
                                                            dataacc[
                                                                5] + '\r\n'
                                            count += 1
                                        else:
                                            logfiledata(folder + newpath, cfg["path"]["error_log"], 'BAJO/', tipe_obu,
                                                        cfg["TIPEOBU"]["SENSOR"][5],
                                                        times, waktu + " " + str(data_a) + '\r\n',
                                                        cfg["path"]["fileraw_txt"])
                                            akuisisi_logger.info("len data obu " + str(int(noOBU)+1) + " bukan 6 saat waktu ke " + waktu)
                                            dataACC = dataBJOeror(tipe_obu, noOBU, waktu)
                                            datafinalacc += dataACC[1]
                                            datarawacc += dataACC[0]

                                    elif tipedata == '2':
                                        # akuisisi_logger.info("data bajo termasuk kategori tipe data 2")
                                        data_raw = waktu + " " + data_a + '\r\n'
                                        logfiledata(folder + newpath, cfg["path"]["raw_log"], 'INKLINO/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][2],
                                                    times,
                                                    data_raw, cfg["path"]["fileraw_txt"])

                                        # parsing_logger.info("Process parsing inklino start")
                                        datainklino = parsingBJO(data_a)
                                        # parsing_logger.info("Process parsing inklino done")
                                        # akuisisi_logger.info("Process pembentukan format data inklino")
                                        datafinalinklino = str(tipe_obu) + " " + cfg["TIPEOBU"]["TIPESENSOR"][
                                            2] + " " + waktu + ' ' + datainklino[2] + ' ' + datainklino[3] + '\r\n'
                                        # print(datafinalinklino)
                                        logfiledata(folder + newpath, cfg["path"]["final_log"], 'INKLINO/', tipe_obu,
                                                    cfg["TIPEOBU"]["SENSOR"][2],
                                                    times, datafinalinklino, cfg["path"]["file_txt"])

                                        queueMQTT[noOBU].put(datafinalinklino)
                                        # akuisisi_logger.info("send data final inklino to queue mqtt")

                                    # elif tipedata == '3':
                                        # akuisisi_logger.info("data bajo termasuk kategori tipe data 3")
                                        # data_raw = waktu + " " + data_a + '\r\n'
                                        # logfiledata(folder + newpath, cfg["path"]["raw_log"], tipe_obu, cfg["TIPEOBU"]["SENSOR"][3],
                                        #             times,
                                        #             data_raw, cfg["path"]["fileraw_txt"])

                                    elif tipedata == '4':
                                        # akuisisi_logger.info("data bajo termasuk kategori tipe data 4")
                                        data_raw = waktu + " " + data_a + '\r\n'
                                        logfiledata(folder + newpath, cfg["path"]["raw_log"], 'ENVIRO/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][4],
                                                    times,
                                                    data_raw,
                                                    cfg["path"]["fileraw_txt"])
                                        # parsing_logger.info("Process parsing enviro start")
                                        dataenviro = parsingBJO(data_a)
                                        # parsing_logger.info("Process parsing enviro done")
                                        # akuisisi_logger.info("Process pembentukan format data enviro")
                                        datafinalenviro = str(tipe_obu) + " " + cfg["TIPEOBU"]["TIPESENSOR"][
                                            4] + " " + waktu + ' ' + dataenviro[2] + ' ' + \
                                                          dataenviro[3] + ' ' + dataenviro[4] + ' ' + dataenviro[5] + '\r\n'
                                        # print(datafinalenviro)
                                        logfiledata(folder + newpath, cfg["path"]["final_log"], 'ENVIRO/', tipe_obu,
                                                    cfg["TIPEOBU"]["SENSOR"][4],
                                                    times, datafinalenviro,
                                                    cfg["path"]["file_txt"])

                                        queueMQTT[noOBU].put(datafinalenviro)
                                        # akuisisi_logger.info("send data final enviro to queue mqtt")

                                    else:
                                        logfiledata(folder + newpath, cfg["path"]["error_log"], 'BAJO/', tipe_obu,
                                                    cfg["TIPEOBU"]["SENSOR"][5],
                                                    times, waktu + " " + data_a + '\r\n',
                                                    cfg["path"]["fileraw_txt"])
                                        akuisisi_logger.info("data bajo obu " + str(int(noOBU)+1) + " tidak termasuk kategori tipe data saat waktu ke " + waktu)
                                else:
                                    logfiledata(folder + newpath, cfg["path"]["error_log"], 'BAJO/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][5],
                                                times, waktu + " " + data_a + '\r\n', cfg["path"]["fileraw_txt"])
                                    akuisisi_logger.info("data bajo obu " + str(int(noOBU)+1) + " hasil checksum false")
                                    dataACC = dataBJOeror(tipe_obu, noOBU, waktu)
                                    datafinalacc += dataACC[1]
                                    datarawacc += dataACC[0]
                            else:
                                logfiledata(folder + newpath, cfg["path"]["error_log"], 'BAJO/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][5],
                                            times,
                                            waktu + " " + data_a + '\r\n', cfg["path"]["fileraw_txt"])
                                akuisisi_logger.info("data raw bajo obu " + str(int(noOBU)+1) + " = '' saat waktu ke " + waktu)
                        else:
                            logfiledata(folder + newpath, cfg["path"]["error_log"], 'BAJO/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][5], times,
                                        waktu + " " + str(dataInput['data']) + '\r\n', cfg["path"]["fileraw_txt"])
                            akuisisi_logger.info("data raw bajo obu " + str(int(noOBU)+1) + " is buffer saat waktu ke " + waktu)
                    # else:
                        # logfiledata(folder + newpath, cfg["path"]["error_log"], 'BAJO/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][5], times, waktu + " " + str(dataInput['data']) + '\r\n', cfg["path"]["fileraw_txt"])
                        # akuisisi_logger.info("data raw bajo obu " + str(int(noOBU)+1) + " double time dan double data saat waktu ke " + waktu)

                    if int((dataInput['ts']).strftime("%S")) != lastTS:
                        lastTS = int((dataInput['ts']).strftime("%S"))
                        logfiledata(folder + newpath, cfg["path"]["raw_log"], 'ACC/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][0], times,
                                    datarawacc, cfg["path"]["fileraw_txt"])
                        logfiledata(folder + newpath, cfg["path"]["final_log"], 'ACC/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][0],
                                    times, datafinalacc, cfg["path"]["file_txt"])
                        logfiledata(folder + newpath, cfg["path"]["final_log"], 'ACC/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][0],
                                    times, 'Data Accelerometer ' + tipe_obu + ' = ' + str(count) + '\r\n',
                                    cfg["path"]["file_txt"])
                        # print(datafinalacc)
                        # print('Data Accelerometer ' + tipe_obu + ' = ' + str(count))
                        queueMQTT[noOBU].put(datafinalacc)
                        # akuisisi_logger.info("send data final acc to queue mqtt")
                        count = 0
                        datafinalacc = ""
                        datarawacc = ""

                except:
                    pass

        except KeyboardInterrupt:
            print('Interrupted')
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)


def prosesBPR(tipe_obu, noOBU):
    # akuisisi_logger.info(cfg["TIPEOBU"]["SENSOR"][1] + '\n')
    beforetime = 0
    waktu_final = 0
    while True:
        try:
            if connectIP[noOBU] == "down":
                beforetime = 0
                akuisisi_logger.info("tidak ada data karena nport atau obu " + str(int(noOBU)+1) + " mati")
                time.sleep(1)
                # epochtime
                t = str((datetime.utcnow()).strftime("%Y%m%d%H%M%S.%f")[:-3])
                d = datetime.strptime(t, "%Y%m%d%H%M%S.%f")
                epoch = datetime.utcfromtimestamp(0)

                def unix_time_millis(dt):
                    return (dt - epoch).total_seconds() * 1000

                waktu = str(int(unix_time_millis(d)))

                data_b = "*0001,9999.990,9999.990"
                datarawbpr = waktu + " " + data_b + '\r\n'
                databpr = parsingBPR(data_b)
                # akuisisi_logger.info("Process pembentukan format data bpr 9999.990")
                datafinalbpr = str(tipe_obu) + " " + cfg["TIPEOBU"]["TIPESENSOR"][0] + " " + waktu + ' ' + databpr[
                    1] + ' ' + databpr[2] + '\r\n'
                # print(datafinalbpr)
                logfiledata(folder + newpath, cfg["path"]["raw_log"], 'BPR/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][1], times,
                            datarawbpr, cfg["path"]["fileraw_txt"])
                logfiledata(folder + newpath, cfg["path"]["final_log"], 'BPR/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][1],
                            times, datafinalbpr, cfg["path"]["file_txt"])
                queueMQTT[noOBU].put(datafinalbpr)
                # akuisisi_logger.info("send data final bpr 9999.990 to queue mqtt")

            elif ket_bpr[noOBU] == "no" and connectIP[noOBU] == "up":
                beforetime = 0
                akuisisi_logger.info("tidak ada data raw obu " + str(int(noOBU)+1) + " bpr di queue maupun dari nport")
                time.sleep(1)
                # epochtime
                t = str((datetime.utcnow()).strftime("%Y%m%d%H%M%S.%f")[:-3])
                d = datetime.strptime(t, "%Y%m%d%H%M%S.%f")
                epoch = datetime.utcfromtimestamp(0)

                def unix_time_millis(dt):
                    return (dt - epoch).total_seconds() * 1000

                waktu = str(int(unix_time_millis(d)))

                data_b = "*0001,9999.900,9999.900"
                datarawbpr = waktu + " " + data_b + '\r\n'
                databpr = parsingBPR(data_b)
                # akuisisi_logger.info("Process pembentukan format data bpr 9999.900")
                datafinalbpr = str(tipe_obu) + " " + cfg["TIPEOBU"]["TIPESENSOR"][0] + " " + waktu + ' ' + databpr[
                    1] + ' ' + \
                               databpr[
                                   2] + '\r\n'
                # print(datafinalbpr)
                logfiledata(folder + newpath, cfg["path"]["raw_log"], 'BPR/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][1], times,
                            datarawbpr, cfg["path"]["fileraw_txt"])
                logfiledata(folder + newpath, cfg["path"]["final_log"], 'BPR/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][1],
                            times, datafinalbpr, cfg["path"]["file_txt"])
                queueMQTT[noOBU].put(datafinalbpr)
                # akuisisi_logger.info("send data final bpr 9999.900 to queue mqtt")

            elif ket_bpr[noOBU] == "yes" and connectIP[noOBU] == "up":
                try:
                    dataInput = queueAllBPROBU[noOBU].get(True, 2)

                    # epochtime
                    t = str((dataInput['ts']).strftime("%Y%m%d%H%M%S.%f")[:-3])
                    d = datetime.strptime(t, "%Y%m%d%H%M%S.%f")
                    epoch = datetime.utcfromtimestamp(0)

                    def unix_time_millis(dt):
                        return (dt - epoch).total_seconds() * 1000

                    waktu = int(unix_time_millis(d))
                    if waktu != beforetime:

                        # sinkronisasi 1 detik waktu bpr
                        if beforetime != 0:
                            waktu_final = beforetime + 1000
                        else:
                            waktu_final = waktu

                        if len(str(dataInput['data']).split("\r\n")) == 2:
                            data_a = str(dataInput['data']).split("\r\n")[0]
                            if data_a != '':
                                # akuisisi_logger.info("data raw bpr tidak ''")
                                if data_a[0] == '*':
                                    # akuisisi_logger.info("header data raw bpr benar")
                                    data_raw = str(waktu_final) + " " + data_a + '\r\n'
                                    logfiledata(folder + newpath, cfg["path"]["raw_log"], 'BPR/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][1], times,
                                                data_raw, cfg["path"]["fileraw_txt"])
                                    databpr = parsingBPR(data_a)

                                    if len(databpr) == 3:
                                        # akuisisi_logger.info("Process pembentukan format data bpr")
                                        data_final = str(tipe_obu) + " " + cfg["TIPEOBU"]["TIPESENSOR"][0] + " " + str(waktu_final) + ' ' + \
                                                     databpr[1] + ' ' + databpr[2] + '\r\n'
                                        # print(data_final)
                                        logfiledata(folder + newpath, cfg["path"]["final_log"], 'BPR/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][1],
                                                    times,
                                                    data_final, cfg["path"]["file_txt"])

                                        queueMQTT[noOBU].put(data_final)
                                        # akuisisi_logger.info("send data final bpr to queue mqtt")
                                    else:
                                        logfiledata(folder + newpath, cfg["path"]["error_log"], 'BPR/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][1], times, str(waktu_final) + " " + data_a + '\r\n', cfg["path"]["fileraw_txt"])
                                        akuisisi_logger.info("len data raw bpr obu " + str(int(noOBU)+1) + " bukan 3 saat waktu ke " + str(waktu_final))
                                        dataBPReror(tipe_obu, noOBU)
                                else:
                                    logfiledata(folder + newpath, cfg["path"]["error_log"], 'BPR/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][1], times, str(waktu_final) + " " + data_a + '\r\n', cfg["path"]["fileraw_txt"])
                                    akuisisi_logger.info("header data raw bpr obu " + str(int(noOBU)+1) + " salah saat waktu ke " + str(waktu_final))
                                    dataBPReror(tipe_obu, noOBU)
                            else:
                                logfiledata(folder + newpath, cfg["path"]["error_log"], 'BPR/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][1], times, str(waktu_final) + " " + data_a + '\r\n', cfg["path"]["fileraw_txt"])
                                akuisisi_logger.info("data raw bpr obu " + str(int(noOBU)+1) + " = '' saat waktu ke " + str(waktu_final))
                        else:
                            logfiledata(folder + newpath, cfg["path"]["error_log"], 'BPR/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][1], times, str(waktu_final) + " " + str(dataInput['data']) + '\r\n', cfg["path"]["fileraw_txt"])
                            akuisisi_logger.info("data raw bpr obu " + str(int(noOBU)+1) + " is buffer saat waktu ke " + str(waktu_final))
                    # else:
                        # logfiledata(folder + newpath, cfg["path"]["error_log"], 'BPR/', tipe_obu, cfg["TIPEOBU"]["SENSOR"][1], times, str(waktu_final) + " " + str(dataInput['data']) + '\r\n', cfg["path"]["fileraw_txt"])
                        # akuisisi_logger.info("data raw bpr obu " + str(int(noOBU)+1) + " double time dan double data saat waktu ke " + str(waktu_final))

                    beforetime = waktu_final
                except:
                    pass

        except KeyboardInterrupt:
            print('Interrupted')
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)


###################################################################################################
# BAJO and BPR Data Processing End
###################################################################################################
###################################################################################################
# TCP Client Start
###################################################################################################
class Client(protocol.Protocol):
    closeCount = 0

    def __init__(self, noOBU, sensorType):
        self.Dictdata = {"ts": 0, "data": ""}
        self.noOBU = noOBU
        self.sensorType = sensorType

    def connectionMade(self):
        print("Connection Made")
        # akuisisi_logger.info("Connection Made")

        reactor.callLater(3.5, self.initBPR, "*0100E4\r\n")
        # akuisisi_logger.info("send command '*0100E4' to sensor BPR")

    def dataReceived(self, data):
        self.countData()
        self.addDatatoQueue(data)
        self.closeCount += 1

    def countData(self):
        if self.sensorType == "BJO":
            BJOPacketCounter[self.noOBU] += 1
        elif self.sensorType == "BPR":
            BPRPacketCounter[self.noOBU] += 1

    def addDatatoQueue(self, data):
        self.Dictdata['ts'] = datetime.utcnow()
        self.Dictdata['data'] = data.decode()

        if self.sensorType == "BJO":
            queueAllBJOOBU[self.noOBU].put(self.Dictdata)
            # akuisisi_logger.info("get data sensor accelerometer obu" + str(self.noOBU))
        elif self.sensorType == "BPR":
            queueAllBPROBU[self.noOBU].put(self.Dictdata)
            # akuisisi_logger.info("get data sensor bpr obu" + str(self.noOBU))

    def initBPR(self, message):
        self.transport.write(message.encode())

    def connectionLost(self, reason):
        print("done")


class ClientFactory(ClFactory):
    def __init__(self, noOBU, sensorType):
        self.noOBU = noOBU
        self.sensorType = sensorType
        self.maxDelay = 1

    def startedConnecting(self, connector):
        print('Started to connect.')
        # akuisisi_logger.info("Started to connect.")
        if cfg["other"]["restart"][self.noOBU] == "True":
            cfg["other"]["restart"][self.noOBU] = "False"
            with open(folder+'config.yaml', 'w') as f:
                cfg_new = yaml.dump(cfg, f)
            if cfg_new is None:
                with open(folder + 'config.yaml', 'w') as f:
                    yaml.dump(cfg, f)

    def buildProtocol(self, addr):
        print('Connected.')
        # akuisisi_logger.info('Connected.')

        print('Resetting reconnection delay')
        akuisisi_logger.info('Resetting reconnection delay')
        self.resetDelay()
        protocol = Client

        return protocol(self.noOBU, self.sensorType)

    def clientConnectionLost(self, connector, reason):
        print("--------------------------------------------")
        print('Connection lost from IP : {} Port : {}\nReason:{}'.format(connector.getDestination().host,
                                                                         connector.getDestination().port, reason))
        akuisisi_logger.error(
            'Connection lost from IP : {} Port : {}\nReason:{}'.format(connector.getDestination().host,
                                                                       connector.getDestination().port, reason))

        ClFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        print('Connection failed from IP : {} Port : {}\nReason:{}'.format(connector.getDestination().host,
                                                                           connector.getDestination().port, reason))
        akuisisi_logger.error(
            'Connection failed from IP : {} Port : {}\nReason:{}'.format(connector.getDestination().host,
                                                                         connector.getDestination().port, reason))

        ClFactory.clientConnectionFailed(self, connector, reason)


###################################################################################################
# TCP Client Stop
###################################################################################################
###################################################################################################
# connect and publish mqtt start
###################################################################################################
def connect_mqtt(tipe, broker, username, password):
    def on_connect(client, userdata, flags, rc):
        # if rc == 0:
        if rc != 0:
            # mqtt_logger.info("Connected to MQTT Broker!")
        # else:
            mqtt_logger.error("Failed to connect, return code")

    # def on_publish(client, userdata, mid):
        # mqtt_logger.info(str(mid) + " = " + tipe + " " + "published")

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    # client.on_publish = on_publish
    client.connect(broker, cfg["mqtt"]["port"])
    return client


def sendData(topic_obu, tipe, queue_mqtt):
    client = []
    for i in range(0, len(cfg["mqtt"]["broker"])):
        client.append(connect_mqtt(tipe, cfg["mqtt"]["broker"][i], cfg["mqtt"]["username"], cfg["mqtt"]["password"]))
        client[i].loop_start()

    while True:
        try:
            data_to_send = queue_mqtt.get()
            logfiledata(folder + newpath, cfg["path"]["final_log"], 'MQTT/', tipe, "mqtt", times, data_to_send, cfg["path"]["file_txt"])
            for i in range(0, len(cfg["mqtt"]["broker"])):
                result = client[i].publish(topic_obu, data_to_send, qos=cfg["mqtt"]["qoslevel"])
                status = result[0]
                # if status == 0:
                if status != 0:
                    # mqtt_logger.info("Send data success to topic server" + str(i))
                # else:
                    mqtt_logger.error("Failed to send data to topic server" + str(i))

        except KeyboardInterrupt:
            print('Interrupted')
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)


def cekKoneksi(IP, noOBU):
    global connectIP
    global ket_bpr
    global ket_bjo
    global newpath
    global tanggal
    global akuisisi_logger
    global times
    # global parsing_logger
    global mqtt_logger
    packetCounterBeforeBPR = [0, 0]
    packetCounterBeforeBJO = [0, 0]
    timesNow = ""
    timesBefore = ""
    dateNow = ""
    dateBefore = ""
    time.sleep(5)
    while True:
        time.sleep(5)
        if noOBU == 0:
            dateNow = str((datetime.utcnow()).strftime("%Y%m%d"))
            timesNow = str((datetime.utcnow()).strftime("%Y%m%d%H"))

            if timesNow != timesBefore:
                times = timesNow
                delete_logger('akuisisi_logger', folder + newpath + cfg["path"]["folder_Log"] + "AKUISISI/" + cfg["TIPEOBU"]["LOKASI"] + cfg["path"]["akuisisi_log"] + times + '.log')
                # delete_logger('parsing_logger', folder + newpath + cfg["path"]["folder_Log"] + "PARSING/" + cfg["TIPEOBU"]["LOKASI"] + cfg["path"]["parsing_log"] + times + '.log')
                delete_logger('mqtt_logger', folder + newpath + cfg["path"]["folder_Log"] + "MQTT/" + cfg["TIPEOBU"]["LOKASI"] + cfg["path"]["mqtt_log"] + times + '.log')

                akuisisi_logger = setup_logger('akuisisi_logger',
                                               folder + newpath + cfg["path"]["folder_Log"] + "AKUISISI/" + cfg["TIPEOBU"][
                                                   "LOKASI"] + cfg["path"]["akuisisi_log"] + times + '.log')
                # parsing_logger = setup_logger('parsing_logger', folder + newpath + cfg["path"]["folder_Log"] + "PARSING/" + cfg["TIPEOBU"]["LOKASI"] + cfg["path"]["parsing_log"] + times + '.log')
                mqtt_logger = setup_logger('mqtt_logger',
                                           folder + newpath + cfg["path"]["folder_Log"] + "MQTT/" + cfg["TIPEOBU"][
                                               "LOKASI"] + cfg["path"]["mqtt_log"] + times + '.log')

            timesBefore = timesNow


            if dateNow != dateBefore:
                makefolder(dateNow + "/")
                newpath = dateNow + "/"
                tanggal = dateNow
                delete_logger('akuisisi_logger', folder + newpath + cfg["path"]["folder_Log"] + "AKUISISI/" + cfg["TIPEOBU"]["LOKASI"] + cfg["path"]["akuisisi_log"] + times + '.log')
                # delete_logger('parsing_logger', folder + newpath + cfg["path"]["folder_Log"] + "PARSING/" + cfg["TIPEOBU"]["LOKASI"] + cfg["path"]["parsing_log"] + times + '.log')
                delete_logger('mqtt_logger', folder + newpath + cfg["path"]["folder_Log"] + "MQTT/" + cfg["TIPEOBU"]["LOKASI"] + cfg["path"]["mqtt_log"] + times + '.log')

                akuisisi_logger = setup_logger('akuisisi_logger', folder + newpath + cfg["path"]["folder_Log"] + "AKUISISI/" + cfg["TIPEOBU"]["LOKASI"] + cfg["path"]["akuisisi_log"] + times + '.log')
                # parsing_logger = setup_logger('parsing_logger', folder + newpath + cfg["path"]["folder_Log"] + "PARSING/" + cfg["TIPEOBU"]["LOKASI"] + cfg["path"]["parsing_log"] + times + '.log')
                mqtt_logger = setup_logger('mqtt_logger', folder + newpath + cfg["path"]["folder_Log"] + "MQTT/" + cfg["TIPEOBU"]["LOKASI"] + cfg["path"]["mqtt_log"] + times + '.log')

            dateBefore = dateNow

        if noOBU < len(cfg["mqtt"]["broker"]):
            resp_rds = ping(cfg["mqtt"]["broker"][noOBU])
            if resp_rds is None:
                mqtt_logger.info("server rds " + cfg["mqtt"]["broker"][noOBU] + " is down")
                print("server rds " + cfg["mqtt"]["broker"][noOBU] + " is down")
                if noOBU == 0:
                    resp_internet = ping(cfg["mqtt"]["internet"])
                    if resp_internet is None:
                        mqtt_logger.info("server rds " + cfg["mqtt"]["broker"][noOBU] + " dan internet is down")
                        print("server rds " + cfg["mqtt"]["broker"][noOBU] + " dan internet is down")
            else:
                # mqtt_logger.info("server rds " + cfg["mqtt"]["broker"][noOBU] + " dan internet is up")
                print("server rds " + cfg["mqtt"]["broker"][noOBU] + " dan internet is up")

        resp = ping(IP)
        if resp is None:
            akuisisi_logger.info("obu " + str(noOBU) + " = " + 'is down')
            print("obu " + str(noOBU) + " = " + 'is down')
            connectIP[noOBU] = "down"
            akuisisi_logger.info("connector disconnect")
            connectorBJO[noOBU].disconnect()
            connectorBPR[noOBU].disconnect()

        else:
            print("obu " + str(noOBU) + " = is up")
            connectIP[noOBU] = "up"
            # akuisisi_logger.info("connection is up")
            if (BPRPacketCounter[noOBU] + BJOPacketCounter[noOBU]) == (
                    packetCounterBeforeBPR[noOBU] + packetCounterBeforeBJO[noOBU]) and (
                    packetCounterBeforeBPR[noOBU] + packetCounterBeforeBJO[noOBU]) != 0 and cfg["other"]["restart"][noOBU] == "False":
                print("obu " + str(noOBU) + " = No Data Received")
                akuisisi_logger.info("No Data Received")
                cfg["other"]["restart"][noOBU] = "True"
                with open(folder+'config.yaml', 'w') as f:
                    cfg_new = yaml.dump(cfg, f)
                if cfg_new is None:
                    with open(folder + 'config.yaml', 'w') as f:
                        yaml.dump(cfg, f)
                akuisisi_logger.info("restart program")
                os.execl(sys.executable, sys.executable, *sys.argv)

            if BPRPacketCounter[noOBU] == packetCounterBeforeBPR[noOBU]:
                ket_bpr[noOBU] = 'no'
            elif BPRPacketCounter[noOBU] != packetCounterBeforeBPR[noOBU]:
                ket_bpr[noOBU] = 'yes'
            else:
                ket_bpr[noOBU] = ''

            if BJOPacketCounter[noOBU] == packetCounterBeforeBJO[noOBU]:
                ket_bjo[noOBU] = 'no'
            elif BJOPacketCounter[noOBU] != packetCounterBeforeBJO[noOBU]:
                ket_bjo[noOBU] = 'yes'
            else:
                ket_bjo[noOBU] = ''

        if (BPRPacketCounter[noOBU] + BJOPacketCounter[noOBU]) != (packetCounterBeforeBPR[noOBU] + packetCounterBeforeBJO[noOBU]) and \
                cfg["other"]["restart"][noOBU] == "True":
            print("obu " + str(noOBU) + " = Data Available")
            # akuisisi_logger.info("Data Available")
            cfg["other"]["restart"][noOBU] = "False"
            with open(folder+'config.yaml', 'w') as f:
                cfg_new = yaml.dump(cfg, f)
            if cfg_new is None:
                with open(folder + 'config.yaml', 'w') as f:
                    yaml.dump(cfg, f)

        if BPRPacketCounter[noOBU] >= 9999:
            BPRPacketCounter[noOBU] = 0
        elif BJOPacketCounter[noOBU] >= 9999:
            BJOPacketCounter[noOBU] = 0
        packetCounterBeforeBPR[noOBU] = BPRPacketCounter[noOBU]
        packetCounterBeforeBJO[noOBU] = BJOPacketCounter[noOBU]


def readCBTParameter():
    global OBUIPList
    global OBUPortList
    global numberofOBU


    OBUIPList = cfg["obuAddressList"]["IP"]
    OBUPortList = cfg["obuAddressList"]["IP"][0], cfg["obuAddressList"]["PORT"]
    numberofOBU = len(OBUIPList)


def findOBUIPList():
    OBUIPList = []
    for i in range(0, len(cfg["obuAddressList"]["IP"])):
        OBUIPList[i] = cfg["obuAddressList"]["IP"][i]


def createBJOQueue(numberofOBU):
    for i in range(0, numberofOBU):
        queueAllBJOOBU.append(Queue(maxsize=1000))

def createMQTTQueue(numberofOBU):
    for i in range(0, numberofOBU):
        queueMQTT.append(Queue(maxsize=1000))

def createBPRQueue(numberofOBU):
    for i in range(0, numberofOBU):
        queueAllBPROBU.append(Queue(maxsize=1000))

def connectOBU():
    i = 0
    for OBUIP in OBUIPList:
        connectorBPR.append(
            reactor.connectTCP(OBUIP, 10002, ClientFactory(i, "BJO")))
        connectorBJO.append(
            reactor.connectTCP(OBUIP, 10001, ClientFactory(i, "BPR")))
        i += 1

def makefolder(path):
    if not os.path.exists(folder + path):
        os.makedirs(folder + path)
    if not os.path.exists(folder + path + cfg["path"]["raw_log"]):
        os.makedirs(folder + path + cfg["path"]["raw_log"])
        os.makedirs(folder + path + cfg["path"]["raw_log"] + "BPR/")
        os.makedirs(folder + path + cfg["path"]["raw_log"] + "ACC/")
        os.makedirs(folder + path + cfg["path"]["raw_log"] + "INKLINO/")
        os.makedirs(folder + path + cfg["path"]["raw_log"] + "ENVIRO/")
    if not os.path.exists(folder + path + cfg["path"]["final_log"]):
        os.makedirs(folder + path + cfg["path"]["final_log"])
        os.makedirs(folder + path + cfg["path"]["final_log"] + "BPR/")
        os.makedirs(folder + path + cfg["path"]["final_log"] + "ACC/")
        os.makedirs(folder + path + cfg["path"]["final_log"] + "INKLINO/")
        os.makedirs(folder + path + cfg["path"]["final_log"] + "ENVIRO/")
        os.makedirs(folder + path + cfg["path"]["final_log"] + "MQTT/")
    if not os.path.exists(folder + path + cfg["path"]["error_log"]):
        os.makedirs(folder + path + cfg["path"]["error_log"])
        os.makedirs(folder + path + cfg["path"]["error_log"] + "BPR/")
        os.makedirs(folder + path + cfg["path"]["error_log"] + "BAJO/")
    if not os.path.exists(folder + path + cfg["path"]["folder_Log"]):
        os.makedirs(folder + path + cfg["path"]["folder_Log"])
        # os.makedirs(folder + path + cfg["path"]["folder_Log"] + "PARSING/")
    if not os.path.exists(folder + path + cfg["path"]["folder_Log"] + "AKUISISI/"):
        os.makedirs(folder + path + cfg["path"]["folder_Log"] + "AKUISISI/")
    if not os.path.exists(folder + path + cfg["path"]["folder_Log"] + "MQTT/"):
        os.makedirs(folder + path + cfg["path"]["folder_Log"] + "MQTT/")

###################################################################################################
# connect and publish mqtt end
###################################################################################################
###################################################################################################
# Main function
###################################################################################################
global connectorBJO
global connectorBPR
global BPRPacketCounter
global BJOPacketCounter
connectorBJO = []
connectorBPR = []
BPRPacketCounter = [0, 0]
BJOPacketCounter = [0, 0]
connectIP = ['', '']
ket_bpr = ['', '']
ket_bjo = ['', '']
newpath = str((datetime.utcnow()).strftime("%Y%m%d")) + "/"
tanggal = str((datetime.utcnow()).strftime("%Y%m%d"))
times = str((datetime.utcnow()).strftime("%Y%m%d%H"))


readCBTParameter()
makefolder(newpath)
akuisisi_logger = setup_logger('akuisisi_logger', folder + newpath + cfg["path"]["folder_Log"] + "AKUISISI/" + cfg["TIPEOBU"]["LOKASI"] + cfg["path"]["akuisisi_log"] + times + '.log')
# parsing_logger = setup_logger('parsing_logger', folder + newpath + cfg["path"]["folder_Log"] + "PARSING/" + cfg["TIPEOBU"]["LOKASI"] + cfg["path"]["parsing_log"] + times + '.log')
mqtt_logger = setup_logger('mqtt_logger', folder + newpath + cfg["path"]["folder_Log"] + "MQTT/" + cfg["TIPEOBU"]["LOKASI"] + cfg["path"]["mqtt_log"] + times + '.log')
akuisisi_logger.info("Start Process")
createBJOQueue(numberofOBU)
createBPRQueue(numberofOBU)
createMQTTQueue(numberofOBU)
connectOBU()


for i in range(0, numberofOBU):
    print(i)
    reactor.callInThread(prosesBJO, cfg["TIPEOBU"]["FINALLOKASI"][i], i)
    reactor.callInThread(prosesBPR, cfg["TIPEOBU"]["FINALLOKASI"][i], i)
    reactor.callInThread(cekKoneksi, cfg["obuAddressList"]["IP"][i], i)
    reactor.callInThread(sendData, cfg["mqtt"]["topic"][i], cfg["TIPEOBU"]["FINALLOKASI"][i], queueMQTT[i])
reactor.run()
