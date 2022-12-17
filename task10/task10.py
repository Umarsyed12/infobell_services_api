import json
import enum
from connect2db import *
from flask import Flask, request, jsonify, Response

app = Flask(__name__)
base = connect_db()
cur = base.cursor()
Host="td_synnex_1"
Disk="sda"
nics="ens4"
percentile=98
start_date="2022-06-22 00:00:00"
end_date="2022-06-28 00:00:00"



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


def create_anomaly_data_cpu(percentile, Host, Disk, nics):
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
    cur.execute(anomaly_query, (percentile, Host, Host, Disk, Host, nics,))
    data = cur.fetchall()
    column_name = [desc[0] for desc in cur.description]
    print("ok2_cpu")
    base.commit()
    return data, column_name


def output_response(data, column_name):
    try:
        resp_dict = {}
        index_len = len(column_name)  # =11
        for index in range(0, index_len):  # (0,11)
            if data[0][index] is None:
                resp_dict.update({column_name[index]: 0})
            else:
                if data[0][index] == data[0][1] or data[0][index] == data[0][11]:
                    resp_dict.update({column_name[index]: str(data[0][index])})
                elif data[0][index] == data[0][0]:
                    resp_dict.update({column_name[index]: data[0][index]})
                else:
                    resp_dict.update({column_name[index]: float(data[0][index])})
        return resp_dict

    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

create_anomaly_cpu(Host, start_date, end_date)
data, column_name = create_anomaly_data_cpu(percentile, Host, Disk, nics)
dict = output_response(data, column_name)
print(dict)




