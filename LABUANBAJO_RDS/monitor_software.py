import os
from datetime import datetime
import yaml
from filelog import setup_logger


path = "C:/Users/USER/Documents/Software_Protokol_Komunikasi/LABUANBAJO_RDS/"

with open(path + "config_monitor.yaml", "r") as ymlfile:
    cfg = yaml.safe_load(ymlfile)
path_tanggal = str((datetime.utcnow()).strftime("%Y%m%d")) + "/"
times = str((datetime.utcnow()).strftime("%Y%m%d%H"))
monitor = "monitor_software_"
akuisisi = "AKUISISI/"
mqtt = "MQTT/"
file_akuisisi = "RKT_akuisisi_log_" + times + ".log"
file_mqtt = "RKT_mqtt_log_" + times + ".log"


def makefolder():
    if not os.path.exists(path + path_tanggal + cfg["path"]["folder_Log"] + "MONITOR/"):
        os.makedirs(path + path_tanggal + cfg["path"]["folder_Log"] + "MONITOR/")
        print("make folder success")
    else:
        print("folder created")


if __name__ == "__main__":
    before_akuisisi = cfg["size"]["akuisisi"]
    before_mqtt = cfg["size"]["mqtt"]

    makefolder()

    monitor_logger = setup_logger('monitor_logger', path + path_tanggal + cfg["path"]["folder_Log"] + "MONITOR/" + monitor + times + '.log')

    if not os.path.exists(path + path_tanggal + cfg["path"]["folder_Log"] + akuisisi + file_akuisisi):
        os.system('systemctl restart akuisisi')
        print("restart service akibat file akuisisi tidak ada")
        monitor_logger.info("restart service akibat file akuisisi tidak ada")
    elif not os.path.exists(path + path_tanggal + cfg["path"]["folder_Log"] + mqtt + file_mqtt):
        os.system('systemctl restart akuisisi')
        print("restart service akibat file mqtt tidak ada")
        monitor_logger.info("restart service akibat file mqtt tidak ada")
    else:
        cek_akuisisi = os.path.getsize(path + path_tanggal + cfg["path"]["folder_Log"] + akuisisi + file_akuisisi)
        cek_mqtt = os.path.getsize(path + path_tanggal + cfg["path"]["folder_Log"] + mqtt + file_mqtt)

        if str(cek_akuisisi) != before_akuisisi:
            monitor_logger.info("size dari file " + file_akuisisi + " sekarang adalah " + str(cek_akuisisi) + " dan sebelumnya adalah " + before_akuisisi + " sehingga software berjalan normal")
            print("size dari file " + file_akuisisi + " sekarang adalah " + str(cek_akuisisi) + " dan sebelumnya adalah " + before_akuisisi + " sehingga software berjalan normal")
        else:
            monitor_logger.info("size dari file " + file_akuisisi + " sekarang adalah " + str(cek_akuisisi) + " dan sebelumnya adalah " + before_akuisisi + " sehingga software sedang hang atau tidak berjalan normal atau sedang dihentikan")
            print("size dari file " + file_akuisisi + " sekarang adalah " + str(
            cek_akuisisi) + " dan sebelumnya adalah " + before_akuisisi + " sehingga software sedang hang atau tidak berjalan normal atau sedang dihentikan")

        if str(cek_mqtt) != before_mqtt:
            monitor_logger.info("size dari file " + file_mqtt + " sekarang adalah " + str(cek_mqtt) + " dan sebelumnya adalah " + before_mqtt + " sehingga software berjalan normal")
            print("size dari file " + file_mqtt + " sekarang adalah " + str(cek_mqtt) + " dan sebelumnya adalah " + before_mqtt + " sehingga software berjalan normal")
        else:
            monitor_logger.info("size dari file " + file_mqtt + " sekarang adalah " + str(cek_mqtt) + " dan sebelumnya adalah " + before_mqtt + " sehingga software sedang hang atau tidak berjalan normal atau sedang dihentikan")
            print("size dari file " + file_mqtt + " sekarang adalah " + str(cek_mqtt) + " dan sebelumnya adalah " + before_mqtt + " sehingga software sedang hang atau tidak berjalan normal atau sedang dihentikan")


        if str(cek_mqtt) == before_akuisisi:
            os.system('systemctl restart akuisisi')
            print("restart service akibat size file mqtt tidak berubah")
            monitor_logger.info("restart service akibat size file mqtt tidak berubah")
        elif str(cek_akuisisi) == before_mqtt:
            os.system('systemctl restart akuisisi')
            print("restart service akibat size file akuisisi tidak berubah")
            monitor_logger.info("restart service akibat size file akuisisi tidak berubah")

        cfg["size"]["mqtt"] = str(cek_mqtt)
        with open(path + 'config_monitor.yaml', 'w') as f:
            yaml.dump(cfg, f)
        cfg["size"]["akuisisi"] = str(cek_akuisisi)
        with open(path + 'config_monitor.yaml', 'w') as f:
            yaml.dump(cfg, f)
