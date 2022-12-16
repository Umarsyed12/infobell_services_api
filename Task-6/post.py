import json
from connect2db import *
from flask import Flask, request, jsonify, Response

app = Flask(__name__)
base = connectDB()
cur = base.cursor()

@app.route('/', methods=['POST'])
def postMark():
    try:
        data = request.json['Name']
        cur.execute(
        "SELECT Total_Marks FROM marks WHERE Name = %s", [data])
        data = cur.fetchone()[0]
        base.commit()
        resp_data = {"Total marks": data}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

if (__name__ == "__main__"):
   app.run(host="0.0.0.0", port=5000, debug=True)