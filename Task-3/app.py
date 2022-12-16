import csv
import json
import psycopg2
import pandas as pd 
from flask import Flask, request, jsonify, Response
app = Flask(__name__)
conn = psycopg2.connect(dbname="test", user="postgres", password="aniket1996", host="localhost", port="5432")

def create_table():
    cur=conn.cursor()
    sql = "DROP TABLE marks"
    cur.execute(sql)
    cur.execute("CREATE TABLE IF NOT EXISTS marks (RollNo TEXT, Name TEXT, Total_Marks TEXT)")
    conn.commit()

def update():
    count=0
    cur=conn.cursor()
    df = pd.read_csv("csv_demo.csv")  
    df['Per'] = ((df['Total_Marks'] / df['Total_Marks'].sum())*100).round(2).astype(str) 
    df.to_csv(r"C:\Users\ANIKET\Downloads\file2.csv", header=True, index=False)
    csv_data = csv.reader(open(r"C:\Users\ANIKET\Downloads\file2.csv"))
    query="ALTER TABLE marks ADD Per TEXT"
    cur.execute(query)
    for row in csv_data:
        if count==0:
            pass
        else:    
            cur.execute("INSERT INTO marks (RollNo, Name, Total_Marks, Per) Values (%s, %s, %s, %s)", row)
        count+=1
    conn.commit()

@app.route('/', methods=['GET', 'POST'])
def load():
    try:
        create_table()
        update()
        resp_data = {"message":"successfull!"}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

if (__name__ == "__main__"):
   app.run(host="0.0.0.0", port=5000, debug=True)

