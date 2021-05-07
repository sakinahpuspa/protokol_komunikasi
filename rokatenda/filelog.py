import logging
import os.path
import os

formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')

def setup_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

def logfiledata(newpath, folderlog, tipeobu, sensor, tanggal, data, tipefile):
    with open(os.path.join(newpath + folderlog, tipeobu + "_" + sensor + "_" + tanggal + tipefile), 'a') as b:
        b.write(data)
