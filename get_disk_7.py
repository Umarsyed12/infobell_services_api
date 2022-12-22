from flask import Flask,Response,request
import json
from connect2db import *
app = Flask(__name__)
base = connect_db()
cur = base.cursor()
@app.route('/disk_name',methods =['GET'])
def disk_name():
    try:
        l = []
        host = request.args.get('host')
        #Diskquery = "select disks from disk where host=%s"
        # where host='td_synnex_4' and disks not like 'loop%%' and disks not like 'dm%%' and disks not like 'sr%%
        cur.execute("select disks from disk where host=%s", (host,))
        data = cur.fetchall()
        print(data)
        for i in data:
            if i[0] not in l:
                l.append(i[0])
        print("host name are :", l)
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