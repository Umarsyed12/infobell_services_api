from flask import Flask, request, jsonify, Response
import json
import csv
import psycopg2

app = Flask(__name__)

conn = psycopg2.connect(
   dbname="test", user="postgres", password="aniket1996", host="localhost", port="5432")

app.config["DEBUG"] = True

@app.route('/', methods=['GET', 'POST'])
def create_table():
    cur=conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS marks (RollNo TEXT, Name TEXT, Total_Marks TEXT)")
    conn.commit()
    return "Table Created Successfully"

@app.route('/upload', methods=['GET', 'POST'])
def load():
    conn = psycopg2.connect(
        dbname="test", user="postgres", password="aniket1996", host="localhost", port="5432")
    cur=conn.cursor()
    
    csv_data = csv.reader(open(r"C:\Users\ANIKET\Downloads\csv_demo.csv"))
    for row in csv_data:
        cur.execute("INSERT INTO marks (RollNo, Name, Total_Marks) Values (%s, %s, %s)", row)
    
    conn.commit()
    conn.close()
    resp_data = {"message":"successfull!"}
    response = json.dumps(resp_data)
    response = Response(response, status=200, mimetype='application/json')
    return  response

@app.route('/show', methods=['GET', 'POST'])
def data():

    csv_file = r"C:\Users\ANIKET\Downloads\csv_demo.csv"
    json_file = r'C:\Users\ANIKET\Downloads\Names.json'

    my_json = {}
    with open(csv_file, 'r') as fobj:
        reader = csv.DictReader(fobj)
        for row in reader:
            key = row['RollNo']
            my_json[key] = row 

    with open(json_file,'w') as fobj:
        fobj.write(json.dumps(my_json, indent=2))

    response = json.dumps(my_json)
    response = Response(response, status=200, mimetype='application/json')
    return  response
    
if (__name__ == "__main__"):
   app.run(host="0.0.0.0", port=5000, debug=True)