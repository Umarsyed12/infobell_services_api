import json
from connect2db import *
from flask import Flask, request, jsonify, Response

app = Flask(__name__)
base = connectDB()
cur = base.cursor()

@app.route('/getNics', methods=['GET','POST'])
def nicsPost():
    try:
        host = request.json["Host"]
        nics = []
        cur.execute(
        "select distinct nics from network where host = %s and nics !='lo' order by nics", [host])
        data = cur.fetchall()
        base.commit()
        for i in data:
            nics.append(i[0])
        count = len(nics)
        resp_data = {"No. of nics": count, "Total nics": nics}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

if (__name__ == "__main__"):
   app.run(host="0.0.0.0", port=5000, debug=True)