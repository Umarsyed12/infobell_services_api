import psycopg2
from flask import Flask, request, jsonify,Response
import json
import csv

app = Flask(__name__)
conn= psycopg2.connect(host="localhost",port=5432,user="postgres",password="postgres",database='infobell')
cur=conn.cursor()

def create_table(header):
    print(header)
    a=header[0]
    b=header[1]
    c=header[2]
    cur.execute(f"CREATE TABLE if not exists marks ({a} int, {b} varchar(20), {c} int);")
    conn.commit()
    print('succesfully added a table')

def alter_table():
    cur.execute("Alter table marks add column percentage float")

def insert_table(v):
    a=v[0]
    b=v[1]
    c=v[2]
    d=(int(c)*100)/1000
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
        print(header,row)
        #conn.commit()
        print("successful")
        resp_data = {"message": "successfull!"}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
