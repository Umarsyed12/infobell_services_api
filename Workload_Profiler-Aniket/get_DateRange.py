import json
from connect2db import *
from flask import Flask, request, jsonify, Response

app = Flask(__name__)
base = connectDB()
cur = base.cursor()

@app.route('/dateRange', methods=['GET','POST'])
def datePost():
    try:
        host = request.json["Host"]
        cur.execute(
        "select date(min(timestamp)) as start_date,date(max(timestamp)) as End_date from cpu where host=%s", [host])
        data = cur.fetchall()
        base.commit()
        resp_data = {"Starting Date" : data[0][0], "End Date" : data[0][1]}
        response = json.dumps(resp_data, default = str)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

if (__name__ == "__main__"):
   app.run(host="0.0.0.0", port=5000, debug=True)