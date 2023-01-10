from connect2db import *
from datetime import datetime

base = connect_db()
mycursor = base.cursor()

# Workload Profiler

def create_anomaly_cpu(host, start_date, end_date):
    global mycursor, base
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
    mycursor.execute(anomaly_query, (host, start_date, end_date))
    print("ok_cpu")
    base.commit()


def create_anomaly_data_cpu(percentile, Host, Disk, nics):
    global mycursor, base
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
    mycursor.execute(anomaly_query, (percentile, Host, Host, Disk, Host, nics,))
    data = mycursor.fetchall()
    column_name = [desc[0] for desc in mycursor.description]
    print("ok2_cpu")
    base.commit()
    return data, column_name


def create_anomaly_Memory(host, start_date, end_date):
    global mycursor, base
    anomaly_query = (''' DROP TABLE If Exists "memory_percentiles";
    CREATE Temp TABLE "memory_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key , 
                                 "memory_utilization" numeric Not Null, "percentile" numeric);

    INSERT INTO "memory_percentiles"("timestamp", "memory_utilization")
    SELECT "timestamp", "utilization" FROM public."memory"
    Where "host" = %s and "timestamp" Between %s and %s
    Order by "utilization" Asc, "timestamp" Desc;

    Update "memory_percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "memory_percentiles"), 5)*100,3);

    ''')
    mycursor.execute(anomaly_query, (host, start_date, end_date))
    print("ok_memory")
    base.commit()


def create_anomaly_data_memory(percentile, Host, Disk, nics):
    global mycursor, base
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
    mycursor.execute(anomaly_query, (percentile, Host, Host, Host, Disk, Host, nics))
    data = mycursor.fetchall()
    column_name = [desc[0] for desc in mycursor.description]
    print("ok2_memory")
    base.commit()
    return data, column_name


def create_anomaly_DiskBusy(host, disk, start_date, end_date):
    global mycursor, base
    anomaly_query = (''' DROP TABLE If Exists "diskbusy_percentiles";
    CREATE Temp TABLE "diskbusy_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key , 
                                 "disk_busy" numeric Not Null, "percentile" numeric);

    INSERT INTO "diskbusy_percentiles"("timestamp", "disk_busy")
    SELECT "timestamp", "busy_percent" FROM public."disk"
    Where "host" = %s and "disks" = %s and "timestamp" Between %s and %s
    Order by "busy_percent" Asc, "timestamp" Desc;

    Update "diskbusy_percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "diskbusy_percentiles"), 5)*100,3);

    '''
                     )
    mycursor.execute(anomaly_query, (host, disk, start_date, end_date))
    print("ok_DiskBusy")
    base.commit()


def create_anomaly_data_DiskBusy(percentile, Host, Disk, nics):
    global mycursor , base
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
    mycursor.execute(anomaly_query, (percentile, Host, Host, Host, Disk, Host, nics))
    data = mycursor.fetchall()
    column_name = [desc[0] for desc in mycursor.description]
    print("ok2_DiskBusy")
    base.commit()
    return data, column_name


def create_anomaly_DiskWeighted(host, disk, start_date, end_date):
    global mycursor , base
    anomaly_query = (''' DROP TABLE If Exists "diskweighted_percentiles";
    CREATE Temp TABLE "diskweighted_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key , 
                                 "disk_weighted" numeric Not Null, "percentile" numeric);

    INSERT INTO "diskweighted_percentiles"("timestamp", "disk_weighted")
    SELECT "timestamp", "weighted_percent" FROM public."disk"
    Where "host" = %s and "disks" = %s and "timestamp" Between %s and %s
    Order by "weighted_percent" Asc, "timestamp" Desc;

    Update "diskweighted_percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "diskweighted_percentiles"), 5)*100,3);
    ''')
    mycursor.execute(anomaly_query, (host, disk, start_date, end_date))
    print("ok_DiskWeighted")
    base.commit()


def create_anomaly_data_DiskWeighted(percentile, Host, Disk, nics):
    global mycursor , base
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
    mycursor.execute(anomaly_query, (percentile, Host, Host, Host, Disk, Host, nics))
    data = mycursor.fetchall()
    column_name = [desc[0] for desc in mycursor.description]
    print("ok2_DiskWeighted")
    base.commit()
    return data, column_name

# benchmark comparison

def create_wl_cpu(workload):
    global mycursor, base
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


def create_wl_data_cpu(percentile, workload, Disk,nics):
    global mycursor, base
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
    mycursor.execute(anomaly_query, (percentile, workload, workload, Disk, workload,nics,))
    data = mycursor.fetchall()
    # print(data)
    column_name = [desc[0] for desc in mycursor.description]
    print("ok2_cpu")
    return data, column_name



def create_wl_Memory(workload):
    global mycursor, base
    anomaly_query = (''' DROP TABLE If Exists "memory_wl_percentiles";
    CREATE Temp TABLE "memory_wl_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key ,
                                 "memory_utilization" numeric Not Null, "percentile" numeric);
    
    INSERT INTO "memory_wl_percentiles"("timestamp", "memory_utilization")
    SELECT "timestamp", "utilization" FROM public."memory"
    Where "workload" = %s
    Order by "utilization" Asc, "timestamp" Desc;
    
    Update "memory_wl_percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "memory_wl_percentiles"), 5)*100,3);
    
    ''')
    mycursor.execute(anomaly_query, (workload,))
    print("ok_memory")
    base.commit()


def create_wl_data_memory(percentile, workload, Disk,nics):
    global mycursor, base
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
    mycursor.execute(anomaly_query, (percentile, workload, workload, workload, Disk, workload,nics,))
    data = mycursor.fetchall()
    column_name = [desc[0] for desc in mycursor.description]
    print("ok2_memory")
    return data, column_name


def create_wl_DiskBusy(workload,disk):
    global mycursor, base
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


def create_wl_data_DiskBusy(percentile, workload, Disk,nics):
    global mycursor, base
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
    mycursor.execute(anomaly_query, (percentile, workload, workload, workload, Disk, workload,nics))
    data = mycursor.fetchall()
    column_name = [desc[0] for desc in mycursor.description]
    print("ok2_DiskBusy")
    return data, column_name


def create_wl_DiskWeighted(workload, disk):
    global mycursor, base
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


def create_wl_data_DiskWeighted(percentile, workload, Disk,nics):
    global mycursor, base
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
    mycursor.execute(anomaly_query, (percentile, workload, workload, workload, Disk, workload,nics))
    data = mycursor.fetchall()
    column_name = [desc[0] for desc in mycursor.description]
    print(column_name)
    print("ok2_DiskWeighted")
    return data, column_name

# specific date

def create_specific_cpu(host, specific):
    global mycursor, base
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


def create_specifi_Memory(host, specific):
    global mycursor, base
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


def create_specific_DiskBusy(host, disk, specific):
    global mycursor, base
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


def create_specific_DiskWeighted(host, disk, specific):
    global mycursor, base
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

ALLOWED_EXTENSIONS = set(['csv'])
ALLOWED_EXTENSIONS = set(['gz'])


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def chekIfHostExists(host):
    cur, engine2, conn = connectToDB()
    
    query = "SELECT host FROM upload_status WHERE host = '{}';".format(host)
    cur.execute(query)
    results = cur.fetchone()
    if results is None:
        return 1
    else:
        for result in results:
            return -1

def insertStatusTbl(filename, uuid_key, host, startTime, endTime, process_msg, status, status_percentage, userName, orgID):
    try:
        cur, engine, conn = connectToDB()
        sql = """INSERT INTO upload_status 
                    (uuid, host, file_name, start_date, end_date, status, message, status_percentage, username, org_id) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        val = (uuid_key, host, filename, startTime, endTime, status, process_msg, status_percentage, userName, orgID)
        cur.execute(sql, val)
        print("inserted progress into update status table. task status:", status_percentage)
        conn.commit()
    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")


def updateStatusTbl(filename, endTime, process_msg, status, status_percentage, start_date):
    try:
        cur, engine, conn = connectToDB()
        query = """UPDATE upload_status
                   SET end_date = %s , status = %s , message = %s , status_percentage = %s
                   WHERE file_name = %s and start_date= %s; """
        cur.execute(query, (endTime, status, process_msg, status_percentage, filename, start_date))
        print("updated progress into update status table. task status:", status_percentage)
        conn.commit()
    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")

def update_ExceptionError(e, new_filename, start_date):
    try:
        endTime = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        process_msg = str(e)
        status = "Failed"
        status_percentage = "100%"
        updateStatusTbl(new_filename, endTime, process_msg, status, status_percentage, start_date)
    
    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")