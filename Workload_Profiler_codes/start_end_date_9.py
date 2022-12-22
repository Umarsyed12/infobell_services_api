from flask import Flask,Response,request
import json
from connect2db import *
app = Flask(__name__)
base = connect_db()
cur = base.cursor()
@app.route('/',methods =['GET'])
def start_endtime():
    try:
        l={}
        cur.execute("select host,date(min(timestamp)) as start_date,date(max(timestamp)) as End_date from cpu group by host;")
        data=cur.fetchall()
        for i in data:
            l.update({str(i[0]):[str(i[1]),str(i[2])]})

        resp_data = {"Timestamp":l}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)