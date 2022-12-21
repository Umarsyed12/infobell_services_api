from flask import Flask,Response
import json
from connect2db import *

app = Flask(__name__)

#base = connectDB()
base = psycopg2.connect(database="practice", host="localhost", user="postgres", password="postgress", port="5432")
cur = base.cursor()

@app.route('/marks')
def unique():
    l = []
    cur.execute("select Total_marks from marks")
    data = cur.fetchall()
    for i in data:
        if i[0] not in l:
            l.append(i[0])
    #response = print("Unique marks: ",l)
    #print("Unique marks: ", l)
    resp_data = {"message": "successful!","Unique":l}
    response = json.dumps(resp_data)
    response = Response(response, status=200, mimetype='application/json')
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

