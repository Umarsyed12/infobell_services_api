from connect2db import *
import tarfile
import json
from datetime import datetime

base = connect_db()
mycursor = base.cursor()


ALLOWED_EXTENSIONS = set(['csv'])
ALLOWED_EXTENSIONS = set(['gz'])


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def unTarFiles(outputTarFile, output_path):
    tarFile = tarfile.open(outputTarFile)
    tarFile.extractall(output_path)
    tarFile.close()

def validate_json(workloadFilePath):
    try:
        if allowed_file(workloadFilePath) :
            with open(workloadFilePath,'r') as file:
                d=json.loads(file.readline())
                key=list(d.keys())
            valid=['timestamp','identity', 'os_release', 'lscpu', 'cpu_total', 'proc_meminfo', 'disks', 'networks']
            for i in valid:
                if i not in key :
                    return jsonify({"message":"file not validated !!"})
            else:
                response = jsonify({"message":"file validated !!"})
                return response
        else:
            return jsonify({"Message": "Invalid file extension!"})
    except Exception as e:
        print(f"\n{'=' *30}\n{e}\n{'=' *30}\n")
        error = {"error": str(e)}
        return jsonify(error)
