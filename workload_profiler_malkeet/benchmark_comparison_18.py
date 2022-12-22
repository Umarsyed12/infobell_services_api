import json
from connect2db import *
from flask import Flask, request, jsonify, Response
from benchmark_function_17 import *
from output_response import *
app = Flask(__name__)
base = connect_db()
mycursor = base.cursor()


def create_wl_cpu(workload):
    anomaly_query = (''' DROP TABLE If Exists "cpu_wl_percentiles";
    CREATE Temp TABLE "cpu_wl_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key ,
                                 "cpu_utilization" numeric Not Null,"percentile" numeric);

    INSERT INTO "cpu_wl_percentiles"("timestamp", "cpu_utilization")
    SELECT "timestamp", "utilization" FROM public."cpu"
    Where "workload" = %s
    Order by "utilization" Asc, "timestamp" Desc;

    Update "cpu_wl_percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "cpu_wl_percentiles"), 5)*100,3);

    '''
                     )
    mycursor.execute(anomaly_query, (workload,))
    print("ok_cpu")
    base.commit()


def create_wl_data_cpu(percentile, workload, Disk, nics):
    anomaly_query = ('''


    Select  A.*,
    C."utilization" as "memory_utilization",
    C."cached_percent" as "cached",
    C."dirty" as "dirty",
    D."busy_percent" as "disk_busy",
    D."weighted_percent" as "disk_weighted",
    E."delta_txbytes" as "txbytes",
    E."delta_rxbytes" as "rxbytes",
    c.workload
    from
    (Select * from "cpu_wl_percentiles"
    Where "percentile" <= %s
    Order By "percentile"  Desc
    Limit 1) A
    Left Join
    (Select * from public."memory" Where "workload" = %s) C
    on A."timestamp" = C."timestamp"
    Left Join
    (Select * from public."disk" Where "workload" = %s and "disks" = %s) D
    on A."timestamp" = D."timestamp"
    Left Join
    (Select * from public."network" Where "workload" = %s and "nics" = %s) E
    on A."timestamp" = E."timestamp";

    ''')
    mycursor.execute(anomaly_query, (percentile, workload, workload, Disk, workload, nics,))
    data = mycursor.fetchall()
    # print(data)
    column_name = [desc[0] for desc in mycursor.description]
    print("ok2_cpu")
    return data, column_name


def create_wl_Memory(workload):
    anomaly_query = (''' DROP TABLE If Exists "memory_wl_percentiles";
    CREATE Temp TABLE "memory_wl_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key ,
                                 "memory_utilization" numeric Not Null, "percentile" numeric);

    INSERT INTO "memory_wl_percentiles"("timestamp", "memory_utilization")
    SELECT "timestamp", "utilization" FROM public."memory"
    Where "workload" = %s
    Order by "utilization" Asc, "timestamp" Desc;

    Update "memory_wl_percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "memory_wl_percentiles"), 5)*100,3);

    '''
                     )
    mycursor.execute(anomaly_query, (workload,))
    print("ok_memory")
    base.commit()


def create_wl_data_memory(percentile, workload, Disk, nics):
    anomaly_query = ('''

    Select  A.*,
    B."utilization" as "cpu_utilization",
    C."cached_percent" as "cached",
    C."dirty" as "dirty",
    D."busy_percent" as "disk_busy",
    D."weighted_percent" as "disk_weighted",
    E."delta_txbytes" as "txbytes",
    E."delta_rxbytes" as "rxbytes",
    c.workload
    from
    (Select * from "memory_wl_percentiles"
    Where "percentile" <= %s
    Order By "percentile"  Desc
    Limit 1) A
    Left Join
    (Select * from public."cpu" Where "workload" = %s   ) B
    on A."timestamp" = B."timestamp"
    Left Join
    (Select * from public."memory" Where "workload" = %s   ) C
    on A."timestamp" = C."timestamp"
    Left Join
    (Select * from public."disk" Where "workload" = %s    and "disks" = %s) D
    on A."timestamp" = D."timestamp"
    Left Join
    (Select * from public."network" Where "workload" = %s    and "nics" = %s) E
    on A."timestamp" = E."timestamp";

    ''')
    mycursor.execute(anomaly_query, (percentile, workload, workload, workload, Disk, workload, nics,))
    data = mycursor.fetchall()
    column_name = [desc[0] for desc in mycursor.description]
    print("ok2_memory")
    return data, column_name


def create_wl_DiskBusy(workload, disk):
    anomaly_query = (''' DROP TABLE If Exists "DiskBusy_wl_percentiles";
    CREATE Temp TABLE "DiskBusy_wl_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key , 
                                 "disk_busy" numeric Not Null, "percentile" numeric);

    INSERT INTO "DiskBusy_wl_percentiles"("timestamp", "disk_busy")
    SELECT "timestamp", "busy_percent" FROM public."disk"
    Where "workload" = %s  and "disks" = %s
    Order by "busy_percent" Asc, "timestamp" Desc;

    Update "DiskBusy_wl_percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "DiskBusy_wl_percentiles"), 5)*100,3);


    '''
                     )
    mycursor.execute(anomaly_query, (workload, disk,))
    print("ok_DiskBusy")
    base.commit()


def create_wl_data_DiskBusy(percentile, workload, Disk, nics):
    mycursor = base.cursor()
    anomaly_query = ('''

    Select  A.*,
    B."utilization" as "cpu_utilization",
    C."utilization" as "memory_utilization",
    C."cached_percent" as "cached", 
    C."dirty" as "dirty",
    D."weighted_percent" as "disk_weighted",
    E."delta_txbytes" as txbytes, 
    E."delta_rxbytes" as rxbytes,
    c.workload
    from 
    (Select * from "DiskBusy_wl_percentiles"
    Where "percentile" <= %s
    Order By "percentile"  Desc
    Limit 1) A
    Left Join 
    (Select * from public."cpu" Where "workload" = %s ) B
    on A."timestamp" = B."timestamp"
    Left Join 
    (Select * from public."memory" Where "workload" = %s ) C
    on A."timestamp" = C."timestamp"
    Left Join 
    (Select * from public."disk" Where "workload" = %s  and "disks" = %s) D
    on A."timestamp" = D."timestamp"
    Left Join 
    (Select * from public."network" Where "workload" = %s and "nics" = %s ) E
    on A."timestamp" = E."timestamp";

    ''')
    mycursor.execute(anomaly_query, (percentile, workload, workload, workload, Disk, workload, nics))
    data = mycursor.fetchall()
    column_name = [desc[0] for desc in mycursor.description]
    print("ok2_DiskBusy")
    return data, column_name


def create_wl_DiskWeighted(workload, disk):
    anomaly_query = (''' DROP TABLE If Exists "DiskWeighted_wl_percentiles";
    CREATE Temp TABLE "DiskWeighted_wl_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key ,
                                 "disk_weighted" numeric Not Null, "percentile" numeric);

    INSERT INTO "DiskWeighted_wl_percentiles"("timestamp", "disk_weighted")
    SELECT "timestamp", "weighted_percent" FROM public."disk"
    Where "workload" = %s  and "disks" = %s
    Order by "weighted_percent" Asc, "timestamp" Desc;

    Update "DiskWeighted_wl_percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "DiskWeighted_wl_percentiles"), 5)*100,3);


    '''
                     )
    mycursor.execute(anomaly_query, (workload, disk,))
    print("ok_DiskWeighted")
    base.commit()


def create_wl_data_DiskWeighted(percentile, workload, Disk, nics):
    anomaly_query = ('''

    Select  A.*,
    B."utilization" as "cpu_utilization",
    C."utilization" as "memory_utilization",
    C."cached_percent" as "cached",
    C."dirty" as "dirty",
    D."busy_percent" as "disk_busy",
    E."delta_txbytes" as txbytes,
    E."delta_rxbytes" as rxbytes,
    c.workload
    from
    (Select * from "DiskWeighted_wl_percentiles"
    Where "percentile" <= %s
    Order By "percentile"  Desc
    Limit 1) A
    Left Join
    (Select * from public."cpu" Where"workload" = %s) B
    on A."timestamp" = B."timestamp"
    Left Join
    (Select * from public."memory" Where "workload" = %s) C
    on A."timestamp" = C."timestamp"
    Left Join
    (Select * from public."disk" Where "workload" = %s  and "disks" =%s) D
    on A."timestamp" = D."timestamp"
    Left Join
    (Select * from public."network" Where "workload" = %s  and "nics" = %s ) E
    on A."timestamp" = E."timestamp";
    ''')
    mycursor.execute(anomaly_query, (percentile, workload, workload, workload, Disk, workload, nics))
    data = mycursor.fetchall()
    column_name = [desc[0] for desc in mycursor.description]
    print(column_name)
    print("ok2_DiskWeighted")
    return data, column_name

'''
def output_response(data, column_name):
    try:
        resp_dict = {}
        #resp_dict.update({"Data_focus": focus})
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
'''

@app.route('/get_workload', methods=['POST'])
def get_workload():
    global mycursor, base
    try:
        workload = request.json['workload']
        Disk = request.json['Disk']
        nics = request.json['nics']
        percentile = str(request.json['percentile'])
        dict={}

        create_wl_cpu(workload)
        focus="Cpu_utilization"
        cpu_data, cpu_column_name=create_wl_data_cpu(percentile,workload,Disk,nics)
        cpu_response=output_response(cpu_data,cpu_column_name,focus)
        dict.update({focus:cpu_response})

        create_wl_Memory(workload)
        focus="memory_utilization"
        memory_data, memory_column_name = create_wl_data_memory(percentile, workload, Disk, nics)
        memory_response = output_response(memory_data, memory_column_name,focus)
        dict.update({focus: memory_response})

        create_wl_DiskBusy(workload,Disk)
        focus="DiskBusy"
        DiskBusy_data, DiskBusy_column_name = create_wl_data_DiskBusy(percentile, workload, Disk, nics)
        DiskBusy_response = output_response(DiskBusy_data, DiskBusy_column_name,focus)
        dict.update({focus: DiskBusy_response})

        create_wl_DiskWeighted(workload,Disk)
        focus = "DiskWeighted_response"
        DiskWeighted_data, DiskWeighted_column_name = create_wl_data_DiskWeighted(percentile, workload, Disk, nics)
        DiskWeighted_response = output_response(DiskWeighted_data, DiskWeighted_column_name,focus)
        dict.update({focus: DiskWeighted_response})

        resp_data = {"Output": dict}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return response

    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)