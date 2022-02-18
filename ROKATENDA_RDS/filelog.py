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

def delete_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)
    logger = logging.getLogger(name)
    handlers = logger.handlers[:]
    for handler in handlers:
        handler.close()
        logger.removeHandler(handler)


def logfiledata(newpath, folderlog, sensorlog, tipeobu, sensor, tanggal, data, tipefile):
    file_object = open(os.path.join(newpath + folderlog + sensorlog, tipeobu + "_" + sensor + "_" + tanggal + tipefile), 'a')
    file_object.write(data)
    file_object.close()
