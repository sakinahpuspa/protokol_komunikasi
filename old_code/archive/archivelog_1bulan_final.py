import zipfile
from datetime import datetime
import shutil

oldpath = str(int((datetime.now()).strftime("%Y%m"))-1)
   
#run crontab tiap sebulan sekali   
#zip file    
zf = zipfile.ZipFile(oldpath + ".zip", "w")
for dirname, subdirs, files in os.walk(oldpath + "/"):
    zf.write(dirname)
    for filename in files:
        zf.write(os.path.join(dirname, filename))
zf.close()
print("zip folder success")
    
#delete folder
shutil.rmtree(oldpath)
print("delete folder")

