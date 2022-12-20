from flask import Flask,Response,request
import json
from Task2 import *
app = Flask(__name__)
base = connect_db()
cur = base.cursor()
@app.route('/',methods =['GET'])
def start_endtime():
    l={}
    cur.execute("select host,date(min(timestamp)) as start_date,date(max(timestamp)) as End_date from cpu group by host;")
    #where host='td_synnex_4' and disks not like 'loop%%' and disks not like 'dm%%' and disks not like 'sr%%
    data=cur.fetchall()
    for i in data:
        l.update({str(i[0]):[str(i[1]),str(i[2])]})

    resp_data = {"Timestamp":l}
    response = json.dumps(resp_data)
    response = Response(response, status=200, mimetype='application/json')
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)