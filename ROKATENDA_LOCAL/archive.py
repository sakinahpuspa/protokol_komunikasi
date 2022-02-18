import os
import shutil
import zipfile
from datetime import datetime

path = "C:/Users/USER/Documents/Software_Protokol_Komunikasi/ROKATENDA_LOCAL/"
path_tanggal = str(int((datetime.utcnow()).strftime("%Y%m%d")) - 1)
path_bulan_zip = str(int((datetime.utcnow()).strftime("%Y%m"))-1)
path_bulan_del = str(int((datetime.utcnow()).strftime("%Y%m"))-2) + ".zip"
path_bulan = str((datetime.utcnow()).strftime("%Y%m"))
tgl30 = ['04', '06', '09', '11']
tgl31 = ['01', '03', '05', '07', '08', '10', '12']
tgl28 = '02'


def makefolder(bln):
    if not os.path.exists(path + bln):
        os.makedirs(path + bln)
        print("make folder success")
    else:
        print("folder created")


def ziptanggal(tgl, bln):
    if os.path.exists(path + tgl):
        zf = zipfile.ZipFile(path + tgl + ".zip", "w")
        for dirname, subdirs, files in os.walk(path + tgl + "/"):
            zf.write(dirname)
            for filename in files:
                zf.write(os.path.join(dirname, filename))
        zf.close()
        print("zip folder tanggal success")

        # delete folder
        shutil.rmtree(path + tgl)
        print("delete folder tanggal")

        shutil.move(path + tgl + ".zip", path + bln + "/" + tgl + ".zip")
        print("move file tanggal zip")
    else:
        print("zip created")


def zip1bulan(bln1):
    if os.path.exists(path + bln1):
        zf = zipfile.ZipFile(path + bln1 + ".zip", "w")
        for dirname, subdirs, files in os.walk(path + bln1 + "/"):
            zf.write(dirname)
            for filename in files:
                zf.write(os.path.join(dirname, filename))
        zf.close()
        print("zip folder 1 bulan success")

        # delete folder
        shutil.rmtree(path + bln1)
        print("delete folder 1 bulan")
    else:
        print("zip 1 bulan created")


def del2bulan(bln2):
    if os.path.exists(path + bln2):
        os.remove(path + bln2)
        print("delete file 2 bulan zip")
    else:
        print("no file 2 bulan zip")


if __name__ == "__main__":
    makefolder(path_bulan)
    if path_tanggal[7] == '0':
        bln3 = str(int(path_tanggal[4:6])-1)
        if bln3 in tgl30:
            path_tanggal = path_tanggal[0:4] + bln3 + '30'
            ziptanggal(path_tanggal, path_tanggal[0:4] + bln3)

        elif bln3 in tgl31:
            path_tanggal = path_tanggal[0:4] + bln3 + '31'
            ziptanggal(path_tanggal, path_tanggal[0:4] + bln3)

        elif bln3 in tgl28:
            path_tanggal = path_tanggal[0:4] + bln3 + '28'
            ziptanggal(path_tanggal, path_tanggal[0:4] + bln3)
    else:
        ziptanggal(path_tanggal, path_bulan)

    zip1bulan(path_bulan_zip)
    del2bulan(path_bulan_del)

