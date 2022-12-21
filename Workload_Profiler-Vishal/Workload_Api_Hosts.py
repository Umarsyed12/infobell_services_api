from flask import Flask,Response,request
import json
from Workload_Connectdb import *

app = Flask(__name__)

base = connect_db()
cur = base.cursor()


@app.route('/hostnames')
def hostnames():
    l = []
    cur.execute("select distinct(host) from cpu order by host")
    data = cur.fetchall()
    for i in data:
        if i[0] not in l:
            l.append(i[0])
    resp_data = {"message": "successful!","Hostnames":l}
    response = json.dumps(resp_data)
    response = Response(response, status=200, mimetype='application/json')
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
