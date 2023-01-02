from flask import Flask, Response
import json
import csv
from Workload_Profiler_Vishvnath.sql.connect2db import*


app = Flask(__name__)

#conn= psycopg2.connect(host="localhost",port=5432,user="postgres",password="postgres",database='project')
conn=connect_db()
cur=conn.cursor()


def create_table(header):

    a=header[0]
    b=header[1]
    c=header[2]
    cur.execute(f"drop table if exists marks")

    cur.execute(f"CREATE TABLE if not exists marks ({a} int, {b} varchar(20), {c} int);")
    conn.commit()
    print('succesfully added a table')
def alter_table():
    cur.execute("Alter table marks add column percentage float")
def insert_table(v):
    for i in range(len(v)):
        a=v[0]
        b=v[1]
        c=v[2]
        d=(int(c)*0.1)
    l=(a,b,c,d)

    cur.execute(f"Insert into marks Values (%s,%s,%s,%s) ",l)

    print('successfully added percentage column in table')
@app.route('/',methods=["GET"])
def api():
    try:
        with open('csv_demo.csv') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            header = next(csv_reader)
            create_table(tuple(header))
            alter_table()
            if header != None:
                # Iterate over each row after the header in the csv
                for row in csv_reader:  # row variable is a list that represents a row in csv
                    insert_table(row)
        #conn.commit()
        print(header,row)

        print("successful")
        resp_data = {"message": "successfull!"}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

@app.route('/kohli',methods=["POST"])
def rohit():
    cur.execute("select Total_Marks from marks where Name ='Virat';")
    marks= cur.fetchall()
    print(marks)
    resp_data = {"Kohli": marks[0][0]}
    response = json.dumps(resp_data)
    response = Response(response, status=200, mimetype='application/json')
    return response

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