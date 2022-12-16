from flask import Flask, render_template, request, jsonify, Response, redirect
import json
import os
import csv
import psycopg2

app = Flask(__name__)

conn = psycopg2.connect(
    dbname="test", user="postgres", password="aniket1996", host="localhost", port="5432")

app.config['FILE_UPLOADS'] = r"C:\Users\ANIKET\OneDrive\Documents\InfoBell\Project\Dec 7\csv_demo.xlsx"


@app.route('/', methods=["GET", "POST"])
def create():
    data = []
    if request.method == 'POST':
        cur = conn.cursor()
        cur.execute('DROP TABLE IF EXISTS marks;')
        cur.execute('CREATE TABLE marks (Roll.NO INTEGER, Name TEXT, Total_Marks INTEGER)')
    return "Table Created Successfully."


@app.route('/upload', methods=["GET", "POST"])
def upload():
    data = {}
    if request.files:
        uploaded_file = request.files['filename']  # This line uses the same variable and worked fine
        filepath = os.path.join(app.config['FILE_UPLOADS'], uploaded_file.filename)
        uploaded_file.save(filepath)
        with open(filepath) as file:
            csv_file = csv.reader(file)
            for row in csv_file:
                data.append(row)
        resp_data = {"message": "Successfully"}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')

    return response


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
