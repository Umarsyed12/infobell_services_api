import json
from connect2db import *
from flask import Flask, request, jsonify, Response

app = Flask(__name__)
base = connectDB()
cur = base.cursor()

@app.route('/getDisks', methods=['GET','POST'])
def diskPost():
    try:
        host = request.json["Host"]
        disks = []
        cur.execute(
        "select distinct(disks) from disk where host=%s and disks not like 'loop%%' and disks not like 'dm%%' and disks not like 'sr%%'", [host])
        data = cur.fetchall()
        base.commit()
        for i in data:
            disks.append(i[0])
        count = len(disks)
        resp_data = {"No. of Disks": count, "Total Disks": disks}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

if (__name__ == "__main__"):
   app.run(host="0.0.0.0", port=5000, debug=True)