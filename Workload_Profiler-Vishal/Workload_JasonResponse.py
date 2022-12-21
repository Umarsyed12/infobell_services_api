from flask import Flask,Response,request
import json
from Task2 import *

app = Flask(__name__)

base = connect_db()
cur = base.cursor()

@app.route('/marks')
def unique():
    l = []
    cur.execute("select Total_marks from marks")
    data = cur.fetchall()
    print(data)
    for i in data:
        print(i)
        if i[0] not in l:
            #print(i)
            l.append(i[0])
            print(l)
    #response = print("Unique marks: ",l)
    print("Unique marks: ", l)
@app.route('/Name_Marks',methods=["POST"])
def rohit_marks():
    Name = request.json['Name']
    query="""select Total_Marks from marks where Name= %s;"""
    cur.execute(query,(Name,))
    marks=cur.fetchall()
    print(marks)
    resp_data = {"message": "successful!",Name:marks[0][0]}
    response = json.dumps(resp_data)
    response = Response(response, status=200, mimetype='application/json')
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)