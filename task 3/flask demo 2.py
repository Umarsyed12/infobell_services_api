import psycopg2
from flask import Flask, request, jsonify,Response
import json
import csv
app = Flask(__name__)
conn= psycopg2.connect(host="localhost",port=5432,user="postgres",password="postgres",database='infobell')
cur=conn.cursor()

table_name='Marks'
data='csv_demo.csv'
def crtbl(tbl):
    cur.execute(f"CREATE TABLE if not exists {tbl}(Roll_NO int, Name varchar(20), Total_Marks int);")
def instbl(tbl,v):
    cur.execute(f"Insert into {tbl} Values {v};")
def add(tbl,f):
    with open(f) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        c = 0
        for row in csv_reader:
            if c == 0:
                c += 1
                continue
            instbl(tbl, tuple(row))
    print('Succesfully added the data')

#@app.route('/display')
def display():
    cur.execute(f"select * from {table_name}")
    rec = cur.fetchall()
    return rec



#print(display())

@app.route('/')
def success():
    crtbl(table_name)
    add(table_name, data)
    cur.close()
    conn.close()
    resp_data = {"message": "successfull!"}
    response = json.dumps(resp_data)
    response = Response(response, status=200, mimetype='application/json')
    return response


if __name__=='__main__':
    app.run(host='0.0.0.0',port=5000, debug=True)

