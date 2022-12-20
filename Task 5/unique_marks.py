import psycopg2
from flask import Flask, request, jsonify,Response
from connect2db import *
import json

app = Flask(__name__)

conn= connect_db()
cur=conn.cursor()

@app.route('/unique',methods=["GET"])
def u_marks():
    cur.execute("select Total_Marks from marks;")
    row=cur.fetchall()
    unique_marks=[]
    for i in row:
        if i[0] not in unique_marks:
            unique_marks.append(i[0])
    return unique_marks

@app.route('/',methods=["POST"])
def post_demo():
    n=request.json['Name']
    cur.execute(f"select Total_Marks from marks where Name='{n}';")
    marks=cur.fetchall()
    print(marks)
    resp_data = {"Total marks " : marks[0][0]}
    response = json.dumps(resp_data)
    response = Response(response, status=200, mimetype='application/json')
    return response


if __name__=='__main__':
    app.run(host='0.0.0.0',port=5000, debug=True)

