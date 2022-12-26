import json
from connect2db import *
from flask import Flask, request, jsonify, Response

app = Flask(__name__)
base = connect_db()
mycursor = base.cursor()

def create_specific_cpu(host, specific):
    anomaly_query = (''' DROP TABLE If Exists "cpu_percentiles";
CREATE Temp TABLE "cpu_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key , 
                             "cpu_utilization" numeric Not Null, "percentile" numeric);

INSERT INTO "cpu_percentiles"("timestamp", "cpu_utilization")
SELECT "timestamp", "utilization" FROM public."cpu"
Where "host"=%s and Date("timestamp")  in %s
Order by "utilization" Asc, "timestamp" Desc;
Update "cpu_percentiles"
Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "cpu_percentiles"), 5)*100,3);
'''
                     )
    mycursor.execute(anomaly_query, (host, specific))
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
    mycursor.execute(anomaly_query, (percentile, Host, Host, Disk, Host,nics,))
    data = mycursor.fetchall()
    column_name = [desc[0] for desc in mycursor.description]
    print("ok2_cpu")
    base.commit()
    return data, column_name

def create_specifi_Memory(host, specific):
    anomaly_query = (''' DROP TABLE If Exists "memory_percentiles";
CREATE Temp TABLE "memory_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key , 
                             "memory_utilization" numeric Not Null, "percentile" numeric);

INSERT INTO "memory_percentiles"("timestamp", "memory_utilization")
SELECT "timestamp", "utilization" FROM public."memory"
Where "host"=%s and Date("timestamp")  in %s
Order by "utilization" Asc, "timestamp" Desc;

Update "memory_percentiles"
Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "memory_percentiles"), 5)*100,3);

'''
                     )
    mycursor.execute(anomaly_query, (host, specific))
    print("ok_memory")
    base.commit()

def create_anomaly_data_memory(percentile, Host, Disk,nics):
    anomaly_query = ('''
    Select A.*, 
    B."utilization" as "cpu_utilization",
    C."cached_percent" as "cached", 
    C."dirty" as "dirty",
    D."busy_percent" as "disk_busy",
    D."weighted_percent" as "disk_weighted",
    E."delta_txbytes" as "txbytes", 
    E."delta_rxbytes" as "rxbytes",
    c.host as workload
    from 
    (Select * from "memory_percentiles"
    Where "percentile" <= %s
    Order By "percentile"  Desc
    Limit 1) A
    Left Join 
    (Select * from public."cpu" Where "host" = %s) B
    on A."timestamp" = B."timestamp"
    Left Join 
    (Select * from public."memory" Where "host" = %s) C
    on A."timestamp" = C."timestamp"
    Left Join 
    (Select * from public."disk" Where "host" = %s and "disks" = %s) D
    on A."timestamp" = D."timestamp"
    Left Join 
    (Select * from public."network" Where "host" = %s and "nics" =%s) E
    on A."timestamp" = E."timestamp";
    ''')
    mycursor.execute(anomaly_query, (percentile, Host, Host, Host, Disk, Host,nics))
    data = mycursor.fetchall()
    column_name = [desc[0] for desc in mycursor.description]
    print("ok2_memory")
    base.commit()
    return data, column_name

def create_specific_DiskBusy(host, disk, specific):
    anomaly_query = (''' DROP TABLE If Exists "diskbusy_percentiles";
CREATE Temp TABLE "diskbusy_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key , 
                             "disk_busy" numeric Not Null, "percentile" numeric);

INSERT INTO "diskbusy_percentiles"("timestamp", "disk_busy")
SELECT "timestamp", "busy_percent" FROM public."disk"
Where "host" = %s and "disks" = %s and Date("timestamp")  in %s
Order by "busy_percent" Asc, "timestamp" Desc;

Update "diskbusy_percentiles"
Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "diskbusy_percentiles"), 5)*100,3);

'''
                     )
    mycursor.execute(anomaly_query, (host, disk, specific))
    print("ok_DiskBusy")
    base.commit()

def create_anomaly_data_DiskBusy(percentile, Host, Disk,nics):
    mycursor = base.cursor()
    anomaly_query = ('''
    Select A.*,
    B."utilization" as "cpu_utilization",
    C."utilization" as "memory_utilization",
    C."cached_percent" as "cached", 
    C."dirty" as "dirty",
    D."weighted_percent" as "disk_weighted",
    E."delta_txbytes" as txbytes, 
    E."delta_rxbytes" as rxbytes,
    c.host as workload
    from 
    (Select * from "diskbusy_percentiles"
    Where "percentile" <= %s
    Order By "percentile"  Desc
    Limit 1) A
    Left Join 
    (Select * from public."cpu" Where "host" = %s) B
    on A."timestamp" = B."timestamp"
    Left Join 
    (Select * from public."memory" Where "host" = %s) C
    on A."timestamp" = C."timestamp"
    Left Join 
    (Select * from public."disk" Where "host" = %s and "disks" = %s) D
    on A."timestamp" = D."timestamp"
    Left Join 
    (Select * from public."network" Where "host" = %s and "nics" = %s) E
    on A."timestamp" = E."timestamp";
    ''')
    mycursor.execute(anomaly_query, (percentile, Host, Host, Host, Disk, Host,nics))
    data = mycursor.fetchall()
    column_name = [desc[0] for desc in mycursor.description]
    print("ok2_DiskBusy")
    base.commit()
    return data, column_name

def create_specific_DiskWeighted(host, disk, specific):
    anomaly_query = (''' DROP TABLE If Exists "diskweighted_percentiles";
CREATE Temp TABLE "diskweighted_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key , 
                             "disk_weighted" numeric Not Null, "percentile" numeric);

INSERT INTO "diskweighted_percentiles"("timestamp", "disk_weighted")
SELECT "timestamp", "weighted_percent" FROM public."disk"
Where "host" = %s and "disks" = %s Date("timestamp")  in %s
Order by "weighted_percent" Asc, "timestamp" Desc;

Update "diskweighted_percentiles"
Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "diskweighted_percentiles"), 5)*100,3);
'''
                     )
    mycursor.execute(anomaly_query, (host, disk, specific))
    print("ok_DiskWeighted")
    base.commit()

def create_anomaly_data_DiskWeighted(percentile, Host, Disk,nics):
    anomaly_query = ('''
    Select A.*,
    B."utilization" as "cpu_utilization",
    C."utilization" as "memory_utilization",
    C."cached_percent" as "cached", 
    C."dirty" as "dirty",
    D."busy_percent" as "disk_busy",
    E."delta_txbytes" as txbytes, 
    E."delta_rxbytes" as rxbytes,
    c.host as workload
    from 
    (Select * from "diskweighted_percentiles"
    Where "percentile" <= %s
    Order By "percentile"  Desc
    Limit 1) A
    Left Join 
    (Select * from public."cpu" Where "host" = %s) B
    on A."timestamp" = B."timestamp"
    Left Join 
    (Select * from public."memory" Where "host" = %s) C
    on A."timestamp" = C."timestamp"
    Left Join 
    (Select * from public."disk" Where "host" = %s and "disks" = %s) D
    on A."timestamp" = D."timestamp"
    Left Join 
    (Select * from public."network" Where "host" = %s and "nics" = %s) E
    on A."timestamp" = E."timestamp";
    ''')
    mycursor.execute(anomaly_query, (percentile, Host, Host, Host, Disk, Host,nics))
    data = mycursor.fetchall()
    column_name = [desc[0] for desc in mycursor.description]
    print("ok2_DiskWeighted")
    base.commit()
    return data, column_name

def output_response(data, column_name,focus):
    try:
        resp_dict = {}
        resp_dict.update({"Data_focus": focus})
        index_len = len(column_name)   #=11
        for index in range(0, index_len): #(0,11)
            if data[0][index] is None:
                resp_dict.update({column_name[index]: 0})
            else:
                if data[0][index]==data[0][1] or data[0][index]==data[0][11] :
                    resp_dict.update({column_name[index]: str(data[0][index])})
                elif data[0][index]==data[0][0]:
                    resp_dict.update({column_name[index]: data[0][index]})
                else:
                    resp_dict.update({column_name[index]: float(data[0][index])})
        return resp_dict

    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

@app.route('/specific_date', methods=['POST'])
def fetchData():
    try:
        Host = request.json['Host']
        Disk = request.json['Disk']
        nics = request.json['nics']
        percentile = request.json['percentile']
        specific_date = request.json['specific_date']
        Data_focus= request.json['Data_focus']
        specific=tuple(specific_date)

        if Data_focus=="Cpu_utilization":
            create_specific_cpu(Host, specific)
            data, column_name = create_anomaly_data_cpu(percentile, Host, Disk,nics)
            print(data)
            dict=output_response(data, column_name,Data_focus)
        elif Data_focus=="Memory_utilization":
            create_specifi_Memory(Host, specific)
            data, column_name = create_anomaly_data_memory(percentile, Host, Disk,nics)
            dict=output_response(data, column_name,Data_focus)
        elif Data_focus=="DiskBusy":
            create_specific_DiskBusy(Host, Disk, specific)
            data, column_name = create_anomaly_data_DiskBusy(percentile, Host, Disk,nics)
            dict=output_response(data, column_name,Data_focus)
        elif Data_focus=="DiskWeighted":
            create_specific_DiskWeighted(Host, Disk, specific)
            data, column_name = create_anomaly_data_DiskWeighted(percentile, Host, Disk,nics)
            dict=output_response(data, column_name,Data_focus)

        resp_data = {"Output": dict}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)