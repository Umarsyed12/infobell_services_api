from flask import Flask,Response
import json
from Workload_Profiler_Vishvnath.sql.connect2db import *
app = Flask(__name__)
base = connect_db()
cur = base.cursor()
@app.route('/disk',methods =['GET'])
def disk_name():
    try:
        l = []
        cur.execute("select disks from disk")
        # where host='td_synnex_4' and disks not like 'loop%%' and disks not like 'dm%%' and disks not like 'sr%%
        data = cur.fetchall()
        print(data)
        for i in data:
            if i[0] not in l:
                l.append(i[0])
        print("disk name are :", l)
        resp_data = {"message": "successful!", "disk": l}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return response
    except Exception as e:
        error = {"error" : "Connection with database is failed"}
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        print(f"\n{'=' * 30}\n{error}\n{'=' * 30}\n")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)