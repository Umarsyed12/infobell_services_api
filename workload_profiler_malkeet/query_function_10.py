from flask import Flask,Response,request
import json
from connect2db import *
app = Flask(__name__)
base = connect_db()
mycursor = base.cursor()



def create_anomaly_cpu(host, start_date, end_date):
    anomaly_query = (''' DROP TABLE If Exists "cpu_percentiles";
    CREATE Temp TABLE "cpu_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key , 
                                 "cpu_utilization" numeric Not Null, "percentile" numeric);

    INSERT INTO "cpu_percentiles"("timestamp", "cpu_utilization")
    SELECT "timestamp", "utilization" FROM public."cpu"
    Where "host" = %s and "timestamp" Between %s and %s
    Order by "utilization" Asc, "timestamp" Desc;

    Update "cpu_percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "cpu_percentiles"), 5)*100,3);
    '''
                     )
    mycursor.execute(anomaly_query, (host, start_date, end_date))
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
    on A."timestamp" = E."timestamp";
    ''')
    mycursor.execute(anomaly_query, (percentile, Host, Host, Disk, Host, nics,))
    data = mycursor.fetchall()
    column_name = [desc[0] for desc in mycursor.description]
    print("ok2_cpu")
    base.commit()
    return data, column_name




