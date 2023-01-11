import os
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from werkzeug.utils import secure_filename
from datetime import datetime
import json
import tarfile


app = Flask(__name__)
CORS(app)

ALLOWED_EXTENSIONS = set(['json','gz'])
def allowed_file(filename):
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def unTarFiles(outputTarFile, output_path):
    tarFile = tarfile.open(outputTarFile)
    tarFile.extractall(output_path)
    tarFile.close()


@app.route('/untar', methods=['POST'])
def untar1():
    try:
        file = request.files.get('file')
        TimeStamp = request.form['TimeStamp']
        filename = secure_filename(file.filename)
        TimeStamp_cleaned = TimeStamp.replace("-", "").replace(" ", "_").replace(":", "")
        new_filename = filename.split("_")[0] + "_" + filename.split("_")[1] + "_" + TimeStamp_cleaned

        if file and allowed_file(file.filename):
            current_directory = os.path.normpath(os.getcwd() + os.sep + os.pardir)
            final_directory = os.path.join(current_directory, "uploaded_files")
            if not os.path.isdir(final_directory):
                os.makedirs(final_directory)
            files_location = os.path.join(final_directory, filename)
            file.save(files_location)
            unTarFiles(files_location, final_directory)
            untar_dir_name = filename.split(".")[0]
            files_dir = os.path.join(final_directory, untar_dir_name)
            workloadFilePath = os.path.join(files_dir, "WorkloadProfile.json")
            platformFilePath = os.path.join(files_dir, "PlatformProfile.json")
            nativePlatformFilePath = os.path.join(files_dir, "nativePlatformDetails.json")
            '''list = os.listdir(files_dir)
            result = {"Output": list[1:4]}
            response = json.dumps(result)
            response = Response(response, status=200, mimetype='application/json')'''
            response = validate_json(workloadFilePath)
            return response

    except Exception as e:
        print(f"\n{'=' *30}\n{e}\n{'=' *30}\n")
        error = {"error": str(e)}
        return jsonify(error)


#@app.route("/validate",methods=['GET'])
def validate_json(workloadFilePath):
    try:
        #files_dir = "D:\infobell\pycharm\poc_pro\\venv\wrapper_idrac-2tmmk93-os_20221202_091059"
        #workloadFilePath = os.path.join(files_dir, "WorkloadProfile.json")
        if allowed_file(workloadFilePath) :
            with open(workloadFilePath,'r') as file:
                #dict.append(JSON.loads(file.readline()))
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
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)