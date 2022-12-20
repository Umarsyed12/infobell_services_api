import json
import enum
from connect2db import *
from flask import Flask, request, jsonify, Response

app = Flask(__name__)
base = connectDB()
cur = base.cursor()

def create_anomaly_cpu(host, start_date, end_date):
    anomaly_query = (''' DROP TABLE If Exists "cpu_percentiles";
    CREATE Temp TABLE "cpu_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null , 
                             "cpu_utilization" numeric Not Null, "percentile" numeric);

    INSERT INTO "cpu_percentiles"("timestamp", "cpu_utilization")
    SELECT "timestamp", "utilization" FROM public."cpu"
    Where "host" = %s and "timestamp" Between %s and %s
    Order by "utilization" Asc, "timestamp" Desc;

    Update "cpu_percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "cpu_percentiles"), 5)*100,3);'''
                     )
    cur.execute(anomaly_query, (host, start_date, end_date))
    print("ok_cpu")
    base.commit()

def create_anomaly_data_cpu(percentile, Host, Disk,nics):
    anomaly_query = ('''
    Select A.*, 
    C."utilization" as "memory_utilization",
    C."cached_percent" as "cached", 
    C."dirty" as "dirty",
    D."busy_percent" as "disk_busy",
    D."weighted_percent" as "disk_weighted",
    E."delta_txbytes" as "txbytes", 
    E."delta_rxbytes" as "rxbytes",
    c.host as workload
    from 
    (Select * from "cpu_percentiles"
    Where "percentile" <= %s
    Order By "percentile"  Desc
    Limit 1) A
    Left Join 
    (Select * from public."memory" Where "host" = %s) C
    on A."timestamp" = C."timestamp"
    Left Join 
    (Select * from public."disk" Where "host" = %s and "disks" = %s) D
    on A."timestamp" = D."timestamp"
    Left Join 
    (Select * from public."network" Where "host" = %s and "nics"= %s ) E
    on A."timestamp" = E."timestamp";''')
    cur.execute(anomaly_query, (percentile, Host, Host, Disk, Host,nics,))
    data = cur.fetchall()
    column_name = [desc[0] for desc in cur.description]
    print("ok2_cpu")
    base.commit()
    return data, column_name

def responce(column_name, data):
    dict = {}
    a=column_name
    b=data
    j, k = 0, 0
    for i in range(len(b)):
        if i==12:
            break
        else:
            dict[a[j]] = b[i][k]
            j+=1
            k+=1
    for i in a:
        if dict[i] is None:
            dict[i] = 0
    return dict

@app.route('/', methods=['POST'])
def fetchData():
    try:
        Host = request.json['Host']
        Disk = request.json['Disk']
        nics = request.json['nics']
        percentile = request.json['percentile']
        start_date = request.json['start_date']
        end_date = request.json['end_date']
        create_anomaly_cpu(Host, start_date, end_date)
        data, column_name = create_anomaly_data_cpu(percentile, Host, Disk, nics)
        dict = responce(column_name, data)
        resp_data = {"Output": dict}
        response = json.dumps(resp_data, default = str)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

if (__name__ == "__main__"):
   app.run(host="0.0.0.0", port=5000, debug=True)