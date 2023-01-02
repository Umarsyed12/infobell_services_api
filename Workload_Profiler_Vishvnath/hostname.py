from flask import Flask,Response
import json
from Workload_Profiler_Vishvnath.sql.connect2db import *
app = Flask(__name__)
base = connect_db()
cur = base.cursor()
@app.route('/host')
def host_name():
    l = []
    cur.execute("select  host from cpu order by host")
    data=cur.fetchall()
    print(data)
    for i in data:
        if i[0] not in l:

            l.append(i[0])
    print("host name are :",l)
    resp_data = {"message": "successful!", "host": l}
    response = json.dumps(resp_data)
    response = Response(response, status=200, mimetype='application/json')
    return response
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)


