from flask import Flask,Response,request
import json
from connect2db import *
app = Flask(__name__)
base = connect_db()
cur = base.cursor()
@app.route('/disk',methods =['GET'])
def nicks_name():
    host = request.args.get('host')
    l = []
    Diskquery = ("select nics from network where host=%s")
    cur.execute(Diskquery, (host,))
    data = cur.fetchall()
    print(data)

    for i in data:
        if i[0] not in l:
            l.append(i[0])
    print("host name are :",l)

    resp_data = {"message": "successful!", "nicks": l}
    response = json.dumps(resp_data)
    response = Response(response, status=200, mimetype='application/json')
    return response
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
