from flask import Flask,Response,request
import json
from Task2 import *
app = Flask(__name__)
base = connect_db()
cur = base.cursor()
@app.route('/workload_disk',methods =['GET'])
def get_disk_workload(): # get bench mark disks from database table
    try:
        l = []
        workload = request.args.get('workload') #spec_cpu_t2d_2vcpu8g_mm
        cur.execute('''select  distinct(disks) from disk where workload=%s and disks not like 'loop%%' and disks not like 'dm%%' and disks not like 'sr%%' order by disks''',(workload,))
        data = cur.fetchall()
        for i in data:
            l.append(i[0])
        resp_data = {"message": "successful!", "disk": l}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return response
    except Exception as e:
        error = {"error" : "Connection with database is failed"}
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        print(f"\n{'=' * 30}\n{error}\n{'=' * 30}\n")
        return jsonify(error)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

