from flask import Flask,Response,request
import json
from connect2db import *
app = Flask(__name__)
base = connect_db()
cur = base.cursor()
@app.route('/workload_nics',methods =['GET'])
def get_nics_workload(): # get bench mark nics from database table
    try:
        l = []
        workload = request.args.get('workload') #spec_cpu_t2d_2vcpu8g_mm
        cur.execute('''select distinct(nics)  from network where workload = %s and nics !='lo' order by nics''',(workload,))
        data = cur.fetchall()
        for i in data:
            l.append(i[0])
        resp_data = {"message": "successful!", "nics": l}
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

