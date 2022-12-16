import csv
import json
import tarfile
import pandas as pd 
from connect2db import *
from flask import Flask, request, jsonify, Response

app = Flask(__name__)
base = connectDB()
cur = base.cursor()

def create_table():
    global cur, base
    sql = "DROP TABLE marks"
    cur.execute(sql)
    cur.execute("CREATE TABLE IF NOT EXISTS marks (RollNo TEXT, Name TEXT, Total_Marks TEXT)")
    base.commit()

def update():
    global cur, base
    df = pd.read_csv("csv_demo.csv")  
    df['Per'] = ((df['Total_Marks'] / df['Total_Marks'].sum())*100).round(2).astype(str) 
    df.to_csv(r"C:\Users\ANIKET\Downloads\file2.csv", header=False, index=False)
    csv_data = csv.reader(open(r"C:\Users\ANIKET\Downloads\file2.csv"))
    query="ALTER TABLE marks ADD Per TEXT"
    cur.execute(query)
    for row in csv_data:    
        cur.execute("INSERT INTO marks (RollNo, Name, Total_Marks, Per) Values (%s, %s, %s, %s)", row)
    base.commit()

@app.route('/', methods=['GET', 'POST'])
def load():
    try:
        create_table()
        update()
        tar()
        resp_data = {"Distinct Marks": distinct()}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

if (__name__ == "__main__"):
   app.run(host="0.0.0.0", port=5000, debug=True)