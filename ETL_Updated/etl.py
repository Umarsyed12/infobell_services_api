import os
import json
from connect2db import *
from collections import OrderedDict
from collections import defaultdict
from query_function import *
from output_response import *
from flask_cors import CORS
from datetime import datetime
from werkzeug.utils import secure_filename
from flask import Flask, request, jsonify, Response


app = Flask(__name__)
CORS(app)
base = connect_db()
mycursor = base.cursor()

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
                validate_json(workloadFilePath)
                list = os.listdir(files_dir)
                result = {"Output" : list[1:4]}
                response = json.dumps(result)
                response = Response(response, status=200, mimetype='application/json')
                return response
        
    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)