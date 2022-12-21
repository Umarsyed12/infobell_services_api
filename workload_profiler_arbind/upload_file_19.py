import os
import csv
import json
import enum
from connect2db import *
from werkzeug.utils import secure_filename
from flask import Flask, request, jsonify, Response

app = Flask(__name__)
base = connect_db()
cur = base.cursor()

ALLOWED_EXTENSIONS = set(['csv'])

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/upload', methods=['POST'])
def upload():
    try:  
        file = request.files['file']
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            new_filename = f'{filename.split(".")[0]}{"."+"csv"}'
            path = r'D:\Dailyworkgit\task19'
            save_location = os.path.join(path, new_filename)
            file.save(save_location)
        else:
            return "Invalid File"
        resp_data = {"Massege": "File Saved Successfully..."}
        response = json.dumps(resp_data, default = str)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

if (__name__ == "__main__"):
   app.run(host="0.0.0.0", port=5000, debug=True)