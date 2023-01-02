import os
import csv
import json
import pandas as pd
from connect2db import *
from flask import Flask, request, jsonify, Response

app = Flask(__name__)
base = connectDB()
cur = base.cursor()

path = r"C:\Users\ANIKET\OneDrive\Documents\InfoBell\Project\Project\InfoBell_Project\csv files\cpu.csv"

def create():
    cur.execute('DROP TABLE IF EXISTS cpu;')
    cur.execute('''CREATE TABLE IF NOT EXISTS public.cpu
(
    " slno" numeric,
    "timestamp" timestamp without time zone,
    cpu text COLLATE pg_catalog."default",
    system_value numeric,
    systems text COLLATE pg_catalog."default",
    last_idle numeric,
    nice numeric,
    irq numeric,
    idle numeric,
    last_total numeric,
    utilization numeric,
    iowait numeric,
    username numeric,
    total numeric,
    softirq numeric,
    upload_date timestamp without time zone,
    tag text COLLATE pg_catalog."default",
    benchmark_tag text COLLATE pg_catalog."default",
    host text COLLATE pg_catalog."default",
    workload text COLLATE pg_catalog."default",
    iteration numeric,
    time_stamp timestamp without time zone,
    is_workload_data numeric,
    guest numeric,
    guestnice numeric,
    steal numeric
)''')

def update():
    df = pd.read_csv(path)  
    csv_data = df.to_csv(path, header=False, index=False)
    for row in csv_data:    
        cur.execute('''INSERT INTO public.cpu(
	" slno", "timestamp", cpu, system_value, systems, last_idle, nice, irq, idle, last_total, utilization, iowait, username, total, softirq, upload_date, tag, benchmark_tag, host, workload, iteration, time_stamp, is_workload_data, guest, guestnice, steal)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);''', row)
    base.commit()

@app.route('/upload', methods=["GET", "POST"])
def upload():
    create()
    update()
    resp_data = {"message": "Successfully"}
    response = json.dumps(resp_data)
    response = Response(response, status=200, mimetype='application/json')    
    return response

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)