import json
from connect2db import *
from flask import Flask, request, jsonify, Response

app = Flask(__name__)
base = connectDB()
cur = base.cursor()

@app.route('/workload', methods=['GET','POST'])
def postMark():
    try:
        hosts = []
        cur.execute(
        "select host from cpu order by host")
        data = cur.fetchall()
        base.commit()
        set_1 = set(data)
        for i in set_1:
            hosts.append(i[0])
        count = len(hosts)
        resp_data = {"No. of hosts": count, "Total Hosts": hosts}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

if (__name__ == "__main__"):
   app.run(host="0.0.0.0", port=5000, debug=True)