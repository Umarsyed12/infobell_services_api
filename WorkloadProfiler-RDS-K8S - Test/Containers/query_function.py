#from connect2db import *
from datetime import datetime
from diskall import *


# base = connect_db()
# mycursor = base.cursor()

mycursor, engine, base = connectToDB()
base.autocommit = True

# Workload Profiler

def create_anomaly_cpu(host, start_date, end_date):
    try:
        global mycursor , base 
        anomaly_query = (''' DROP TABLE If Exists "cpu_percentiles";
        CREATE temp TABLE "cpu_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null , 
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
        
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)


def create_anomaly_data_cpu(percentile, Host, Disk, nics):
    try:
        global mycursor , base
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
    
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)


def create_anomaly_Memory(host, start_date, end_date):
    try:
        global mycursor , base
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
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

def create_anomaly_data_memory(percentile, Host, Disk, nics):
    try:
        global mycursor , base
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
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)


def create_anomaly_DiskBusy(host, disk, start_date, end_date):
    try:
        global mycursor , base
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
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

def create_anomaly_data_DiskBusy(percentile, Host, Disk, nics):
    try:
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
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

def create_anomaly_DiskWeighted(host, disk, start_date, end_date):
    try:
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
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)


def create_anomaly_data_DiskWeighted(percentile, Host, Disk, nics):
    try:
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
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)
    
    
# benchmark comparison

def create_wl_cpu(workload):
    try:
        global mycursor , base
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
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

def create_wl_data_cpu(percentile, workload, Disk,nics):
    try:
        global mycursor , base
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
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)



def create_wl_Memory(workload):
    try:
        global mycursor , base
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
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)


def create_wl_data_memory(percentile, workload, Disk,nics):
    try:
        global mycursor , base
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
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)


def create_wl_DiskBusy(workload,disk):
    try:
        global mycursor , base
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
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

def create_wl_data_DiskBusy(percentile, workload, Disk,nics):
    try:
        global mycursor , base
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
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)


def create_wl_DiskWeighted(workload, disk):
    try:
        global mycursor , base
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
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

def create_wl_data_DiskWeighted(percentile, workload, Disk,nics):
    try:
        global mycursor , base
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
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

# specific date

def create_specific_cpu(host, specific):
    try:
        global mycursor , base
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
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

def create_specifi_Memory(host, specific):
    try:
        global mycursor , base
        
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
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

def create_specific_DiskBusy(host, disk, specific):
    try:
        global mycursor , base
        
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
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

def create_specific_DiskWeighted(host, disk, specific):
    try:
        global mycursor , base
        
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
    except Exception as e:
        mycursor, engine, base = connectToDB()
        mycursor.execute("ROLLBACK")
        base.commit()
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)


