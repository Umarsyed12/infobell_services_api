from utils import *

base = connectDB()
cur = base.cursor()

# QUERY FUNCTIONS

# Workload Profiler
def create_anomaly_cpu(host, start_date, end_date):
    global cur, base
    anomaly_query = (''' DROP TABLE If Exists "cpu_percentiles";
    CREATE Temp TABLE "cpu_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null , 
                             "cpu_utilization" numeric Not Null, "percentile" numeric);

    INSERT INTO "cpu_percentiles"("timestamp", "cpu_utilization")
    SELECT "timestamp", "utilization" FROM public."cpu"
    Where "host" = %s and "timestamp" Between %s and %s Order by "utilization" Asc, "timestamp" Desc;

    Update "cpu_percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "cpu_percentiles"), 5)*100,3);''')
    cur.execute(anomaly_query, (host, start_date, end_date))
    print("ok_cpu")
    base.commit()


def create_anomaly_data_cpu(percentile, Host, Disk, nics):
    global cur, base
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
    Order By "percentile" Desc
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

def create_anomaly_Memory(host, start_date, end_date):
    global cur, base
    anomaly_query = (''' DROP TABLE If Exists "memory_percentiles";
    CREATE Temp TABLE "memory_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key , 
                                 "memory_utilization" numeric Not Null, "percentile" numeric);

    INSERT INTO "memory_percentiles"("timestamp", "memory_utilization")
    SELECT "timestamp", "utilization" FROM public."memory"
    Where "host" = %s and "timestamp" Between %s and %s
    Order by "utilization" Asc, "timestamp" Desc;

    Update "memory_percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "memory_percentiles"), 5)*100,3);''')
    cur.execute(anomaly_query, (host, start_date, end_date))
    print("ok_memory")
    base.commit()

def create_anomaly_data_memory(percentile, Host, Disk, nics):
    global cur, base
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
    on A."timestamp" = E."timestamp";''')
    cur.execute(anomaly_query, (percentile, Host, Host, Host, Disk, Host, nics))
    data = cur.fetchall()
    column_name = [desc[0] for desc in cur.description]
    print("ok2_memory")
    base.commit()
    return data, column_name

def create_anomaly_DiskBusy(host, disk, start_date, end_date):
    global cur, base
    anomaly_query = (''' DROP TABLE If Exists "diskbusy_percentiles";
    CREATE Temp TABLE "diskbusy_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key , 
                                 "disk_busy" numeric Not Null, "percentile" numeric);

    INSERT INTO "diskbusy_percentiles"("timestamp", "disk_busy")
    SELECT "timestamp", "busy_percent" FROM public."disk"
    Where "host" = %s and "disks" = %s and "timestamp" Between %s and %s
    Order by "busy_percent" Asc, "timestamp" Desc;

    Update "diskbusy_percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "diskbusy_percentiles"), 5)*100,3);''')
    cur.execute(anomaly_query, (host, disk, start_date, end_date))
    print("ok_DiskBusy")
    base.commit()

def create_anomaly_data_DiskBusy(percentile, Host, Disk, nics):
    global cur , base
    cur = base.cursor()
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
    on A."timestamp" = E."timestamp";''')
    cur.execute(anomaly_query, (percentile, Host, Host, Host, Disk, Host, nics))
    data = cur.fetchall()
    column_name = [desc[0] for desc in cur.description]
    print("ok2_DiskBusy")
    base.commit()
    return data, column_name

def create_anomaly_DiskWeighted(host, disk, start_date, end_date):
    global cur , base
    anomaly_query = (''' DROP TABLE If Exists "diskweighted_percentiles";
    CREATE Temp TABLE "diskweighted_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key , 
                                 "disk_weighted" numeric Not Null, "percentile" numeric);

    INSERT INTO "diskweighted_percentiles"("timestamp", "disk_weighted")
    SELECT "timestamp", "weighted_percent" FROM public."disk"
    Where "host" = %s and "disks" = %s and "timestamp" Between %s and %s
    Order by "weighted_percent" Asc, "timestamp" Desc;

    Update "diskweighted_percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "diskweighted_percentiles"), 5)*100,3);''')
    cur.execute(anomaly_query, (host, disk, start_date, end_date))
    print("ok_DiskWeighted")
    base.commit()

def create_anomaly_data_DiskWeighted(percentile, Host, Disk, nics):
    global cur , base
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
    on A."timestamp" = E."timestamp";''')
    cur.execute(anomaly_query, (percentile, Host, Host, Host, Disk, Host, nics))
    data = cur.fetchall()
    column_name = [desc[0] for desc in cur.description]
    print("ok2_DiskWeighted")
    base.commit()
    return data, column_name

# benchmark comparison

def create_wl_cpu(workload):
    global cur, base
    anomaly_query = (''' DROP TABLE If Exists "cpu_wl_percentiles";
    CREATE Temp TABLE "cpu_wl_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key ,
                                 "cpu_utilization" numeric Not Null,"percentile" numeric);
    
    INSERT INTO "cpu_wl_percentiles"("timestamp", "cpu_utilization")
    SELECT "timestamp", "utilization" FROM public."cpu"
    Where "workload" = %s
    Order by "utilization" Asc, "timestamp" Desc;
    
    Update "cpu_wl_percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "cpu_wl_percentiles"), 5)*100,3);''')
    cur.execute(anomaly_query, (workload,))
    print("ok_cpu")
    base.commit()

def create_wl_data_cpu(percentile, workload, Disk,nics):
    global cur, base
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
    on A."timestamp" = E."timestamp";''')
    cur.execute(anomaly_query, (percentile, workload, workload, Disk, workload,nics,))
    data = cur.fetchall()
    # print(data)
    column_name = [desc[0] for desc in cur.description]
    print("ok2_cpu")
    return data, column_name

def create_wl_Memory(workload):
    global cur, base
    anomaly_query = (''' DROP TABLE If Exists "memory_wl_percentiles";
    CREATE Temp TABLE "memory_wl_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key ,
                                 "memory_utilization" numeric Not Null, "percentile" numeric);
    
    INSERT INTO "memory_wl_percentiles"("timestamp", "memory_utilization")
    SELECT "timestamp", "utilization" FROM public."memory"
    Where "workload" = %s
    Order by "utilization" Asc, "timestamp" Desc;
    
    Update "memory_wl_percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "memory_wl_percentiles"), 5)*100,3);''')
    cur.execute(anomaly_query, (workload,))
    print("ok_memory")
    base.commit()

def create_wl_data_memory(percentile, workload, Disk,nics):
    global cur, base
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
    on A."timestamp" = E."timestamp";''')
    cur.execute(anomaly_query, (percentile, workload, workload, workload, Disk, workload,nics,))
    data = cur.fetchall()
    column_name = [desc[0] for desc in cur.description]
    print("ok2_memory")
    return data, column_name

def create_wl_DiskBusy(workload,disk):
    global cur, base
    anomaly_query = (''' DROP TABLE If Exists "DiskBusy_wl_percentiles";
    CREATE Temp TABLE "DiskBusy_wl_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key , 
                                 "disk_busy" numeric Not Null, "percentile" numeric);
    
    INSERT INTO "DiskBusy_wl_percentiles"("timestamp", "disk_busy")
    SELECT "timestamp", "busy_percent" FROM public."disk"
    Where "workload" = %s  and "disks" = %s
    Order by "busy_percent" Asc, "timestamp" Desc;
    
    Update "DiskBusy_wl_percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "DiskBusy_wl_percentiles"), 5)*100,3);''')
    cur.execute(anomaly_query, (workload, disk,))
    print("ok_DiskBusy")
    base.commit()

def create_wl_data_DiskBusy(percentile, workload, Disk,nics):
    global cur, base
    cur = base.cursor()
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
    on A."timestamp" = E."timestamp";''')
    cur.execute(anomaly_query, (percentile, workload, workload, workload, Disk, workload,nics))
    data = cur.fetchall()
    column_name = [desc[0] for desc in cur.description]
    print("ok2_DiskBusy")
    return data, column_name

def create_wl_DiskWeighted(workload, disk):
    global cur, base
    anomaly_query = (''' DROP TABLE If Exists "DiskWeighted_wl_percentiles";
    CREATE Temp TABLE "DiskWeighted_wl_percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key ,
                                 "disk_weighted" numeric Not Null, "percentile" numeric);
    
    INSERT INTO "DiskWeighted_wl_percentiles"("timestamp", "disk_weighted")
    SELECT "timestamp", "weighted_percent" FROM public."disk"
    Where "workload" = %s  and "disks" = %s
    Order by "weighted_percent" Asc, "timestamp" Desc;
    
    Update "DiskWeighted_wl_percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "DiskWeighted_wl_percentiles"), 5)*100,3);''')
    cur.execute(anomaly_query, (workload, disk,))
    print("ok_DiskWeighted")
    base.commit()

def create_wl_data_DiskWeighted(percentile, workload, Disk,nics):
    global cur, base
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
    on A."timestamp" = E."timestamp";''')
    cur.execute(anomaly_query, (percentile, workload, workload, workload, Disk, workload,nics))
    data = cur.fetchall()
    column_name = [desc[0] for desc in cur.description]
    print(column_name)
    print("ok2_DiskWeighted")
    return data, column_name

# specific date

def create_specific_cpu(host, specific):
    global cur, base
    anomaly_query = (''' DROP TABLE If Exists "percentiles";
    CREATE Temp TABLE "percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key , 
                                 "cpu_utilization" numeric Not Null, "percentile" numeric);
    
    INSERT INTO "percentiles"("timestamp", "cpu_utilization")
    SELECT "timestamp", "utilization" FROM public."cpu"
    Where "host"=%s and Date("timestamp")  in %s
    Order by "utilization" Asc, "timestamp" Desc;
    Update "percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "percentiles"), 5)*100,3);''')
    cur.execute(anomaly_query, (host, specific))
    print("ok_cpu")
    base.commit()

def create_specifi_Memory(host, specific):
    global cur, base
    anomaly_query = (''' DROP TABLE If Exists "percentiles";
    CREATE Temp TABLE "percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key , 
                                 "memory_utilization" numeric Not Null, "percentile" numeric);
    
    INSERT INTO "percentiles"("timestamp", "memory_utilization")
    SELECT "timestamp", "utilization" FROM public."memory"
    Where "host"=%s and Date("timestamp")  in %s
    Order by "utilization" Asc, "timestamp" Desc;
    
    Update "percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "percentiles"), 5)*100,3);''')
    cur.execute(anomaly_query, (host, specific))
    print("ok_memory")
    base.commit()

def create_specific_DiskBusy(host, disk, specific):
    global cur, base
    anomaly_query = (''' DROP TABLE If Exists "percentiles";
    CREATE Temp TABLE "percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key , 
                                 "disk_busy" numeric Not Null, "percentile" numeric);
    
    INSERT INTO "percentiles"("timestamp", "disk_busy")
    SELECT "timestamp", "busy_percent" FROM public."disk"
    Where "host" = %s and "disks" = %s and Date("timestamp")  in %s
    Order by "busy_percent" Asc, "timestamp" Desc;
    
    Update "percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "percentiles"), 5)*100,3);''')
    cur.execute(anomaly_query, (host, disk, specific))
    print("ok_DiskBusy")
    base.commit()

def create_specific_DiskWeighted(host, disk, specific):
    global cur, base
    anomaly_query = (''' DROP TABLE If Exists "percentiles";
    CREATE Temp TABLE "percentiles"(Rank SERIAL,"timestamp" timestamp without time zone Not Null Primary key , 
                                 "disk_weighted" numeric Not Null, "percentile" numeric);
    
    INSERT INTO "percentiles"("timestamp", "disk_weighted")
    SELECT "timestamp", "weighted_percent" FROM public."disk"
    Where "host" = %s and "disks" = %s Date("timestamp")  in %s
    Order by "weighted_percent" Asc, "timestamp" Desc;
    
    Update "percentiles"
    Set "percentile" = Round(Round(Cast(Rank AS Decimal) /(Select count(*) from "percentiles"), 5)*100,3);''')
    cur.execute(anomaly_query, (host, disk, specific))
    print("ok_DiskWeighted")
    base.commit()


# ETL FUNCTIONS
def connectToDB():
    try:
        conn = psycopg2.connect(
            database = "Services",
            user = "postgres",
            password = "postgress",
            host = "localhost",
            port = "5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        engine = create_engine("postgresql+psycopg2://postgres:%s@localhost/Services" % urlquote("postgress"))
        return cur, engine, conn
    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

# ALLOWED_EXTENSIONS = set(['csv', 'json'])
ALLOWED_EXTENSIONS = set(['gz'])
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

def unTarFiles(outputTarFile, output_path):
    tarFile = tarfile.open(outputTarFile)
    tarFile.extractall(output_path)
    tarFile.close()

def validate_workload_json(new_filename, workloadFilePath, start_date):
    dict_list_final = []
    try:
        with open(workloadFilePath, 'r') as file:
            dict_list_final.append(JSON.loads(file.readline()))

        df = pd.DataFrame(dict_list_final)
        json_cols = list(df)
        njmon_cols = ['timestamp', 'cpu_total', 'proc_meminfo', 'disks', 'networks']

        if any(x in njmon_cols for x in json_cols):
            endTime = None
            process_msg = "File Validated "
            status = "In progress"
            status_percentage = "28%"
            updateStatusTbl(new_filename, endTime, process_msg, status, status_percentage, start_date)

            workload_response = jsonify({"Message": "File copied to system Successfully!", "status": 1})
        else:
            endTime = None
            process_msg = "Failed to validate file"
            status = "Failed"
            status_percentage = "100%"
            updateStatusTbl(new_filename, endTime, process_msg, status, status_percentage, start_date)
            workload_response = jsonify({"Message": "The attached file is not a valid njmon json format", "status": -1})

    except ValueError as err:
        workload_response = jsonify({"Message": "The attached file is not a valid njmon json format", "status": -1})

    return workload_response

def workload_etl(benchmark_tag, uuid_key, host, tag, workload, iteration, workloadFilePath, is_workload_data,
                 new_filename, start_date):
    endTime = None
    process_msg = "Started ETL Process"
    status = "Running"
    status_percentage = "30%"
    updateStatusTbl(new_filename, endTime, process_msg, status, status_percentage, start_date)

    print("args:", "host:", host, "workload:", workload, "iteration", iteration, "filename:", workloadFilePath,
          "is_workload_data:", is_workload_data)
    print(".............host:", host, "started..................")
    time.sleep(1)

    ########### get perfect json ###########
    dict_list_final = convert_to_json(workloadFilePath)

    ########## load_with_tags #############
    table_list = ['cpu_total', 'disks', 'networks', 'proc_meminfo']
    args_list = []

    print("loading")
    for table in table_list:
        df_table = load_with_tags(dict_list_final, uuid_key, benchmark_tag, host, workload, iteration, is_workload_data,
                                  tag, table)
        print("len(df_{}) :".format(table), len(df_table))
        table_list = [df_table, table, host, new_filename, start_date]
        args_list.append(table_list)

    ########## Pipeline Arguments #########
    runInParallel(args_list)

    response = getWorkloadReturnVal(host)
    return response

NULL = 0
completed_list = []
global startTime, endTime

def pipeline(arg):
    try:
        print("\n***** Started for", arg[2], "*****")
        startp = time.time()
        df_Measurement = arg[0]
        measure_type = arg[1]
        host = arg[2]
        new_filename = arg[3]
        start_date = arg[4]

        ########### filter ##################
        json_dict = get_filtered_data(df_Measurement)

        ########### Processing ##############
        table_json_array = process(json_dict, measure_type)
        print(
            "\n***** Transformed: host: {}: Total: {} for: {} *****".format(host, len(table_json_array), measure_type))
        df = convert_to_csv(table_json_array)
        df = normalize(df, measure_type)
        df = restructure_for_sql(df, measure_type)

        ########### csv creation ##################
        # timestamp = str(datetime.now().strftime("%d%m%Y%H%M%S"))
        # outputpath = os.path.join(os.getcwd(), "output")
        # generated_path = os.path.join(outputpath, str(host))
        # if not os.path.exists(generated_path):
        #     os.makedirs(generated_path)
        #     print(f"{generated_path} folder created.")
        # else:
        #     print(f"{generated_path} folder exists.")
        # outputfile = os.path.join(generated_path, str(host)+"_"+ timestamp +"_"+ measure_type +".csv")

        # df.to_csv(outputfile)

        ########### insert to db ###########
        ingestData(df, measure_type)

        endTime = None
        process_msg = "ETL under process"
        status = "Running"
        if (measure_type == 'cpu_total'):
            status_percentage = "42%"
        elif (measure_type == 'disks'):
            status_percentage = "55%"
        elif (measure_type == 'networks'):
            status_percentage = "67%"
        elif (measure_type == 'proc_meminfo'):
            status_percentage = "80%"
        updateStatusTbl(new_filename, endTime, process_msg, status, status_percentage, start_date)

        print("\n-***** Completed: host: {}: total: {} for: {} in time s: {} *****".format(host, len(table_json_array),
                                                                                           str(measure_type),
                                                                                           time.time() - startp))
        list_val = host + "_" + measure_type
        completed_list.append(list_val)
        print("completed_list: ", completed_list)
        completed_list.clear()
        return 1
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(" line number: {}, error : {}".format(exc_tb.tb_lineno, e))
        return 0

def runInParallel(args_list):
    for arg in args_list:
        t = threading.Thread(target=pipeline, args=[arg])
        t.start()
        t.join()

def convert_to_json(file):
    with open(file) as f:
        dict_list = f.readlines()
    dict_list_final = []
    for str_dict in dict_list:
        dict_list_final.append(json.loads(str_dict))
    return dict_list_final

def prep_time(txt):
    return txt.split('.')[0].replace('T', ' ')

def load_with_tags(dict_list_final, uuid_key, benchmark_tag, host, workload, iteration, is_workload_data, tag,
                   df_table_name):
    # Create a DF
    df = pd.DataFrame(dict_list_final)
    print(len(df))
    df_table = pd.DataFrame(list(df[df_table_name].values))

    pk_val = []
    for i in range(len(df)):
        pk = 'wp_' + ''.join((random.choice(string.ascii_lowercase) for x in range(5))) + "_" + str(
            datetime.now().strftime("%d%m%Y%H%M%S"))
        pk_val.append(pk)

    if df_table_name == "cpu_total":
        df_table['systems'] = '/proc/stat'
        df_table['cpu'] = 'cpu'
    elif df_table_name == "proc_meminfo":
        df_table['systems'] = '/proc/meminfo'
    elif df_table_name == "networks":
        df_table['systems'] = '/proc/net/dev'
    elif df_table_name == "disks":
        df_table['systems'] = '/proc/diskstats'

    df_timestamp_val = pd.DataFrame(list(df['timestamp'].values))
    df_table['pkey'] = [_ for _ in pk_val]
    df_table['uuid'] = uuid_key
    df_table['time_stamp'] = df_timestamp_val['datetime'].apply(prep_time)
    df_table['benchmark_tag'] = benchmark_tag
    df_table['host'] = host
    df_table['workload'] = workload
    df_table['iteration'] = iteration
    df_table['tag'] = tag
    df_table['is_workload_data'] = is_workload_data
    df_table['upload_date'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return df_table

def get_filtered_data(df_table):
    df_tmp = df_table
    df_tmp.reset_index(drop=True, inplace=True)
    json_dict = df_tmp.to_dict('records')
    print("number of records:", len(json_dict))
    return json_dict

def convert_to_csv(transformed_array):
    dict_list = []
    for trans_dict in transformed_array:
        for i in range(len(trans_dict.get('data'))):
            dict_list.append(trans_dict.get('data')[i].copy())
    df = pd.DataFrame(dict_list)
    return df

def normalization(df, attribute):
    # zi = (xi – min(x)) / (max(x) – min(x)) * 100
    # Dirty, delta_obytes, delta_ibytes
    minm = min(df[attribute].values)
    maxm = max(df[attribute].values)
    # df[attribute+'_Norm'] = df[attribute].apply(lambda x: (x - minm // (maxm - minm)) )
    df[attribute + '_norm'] = ((df[attribute] - minm) / (maxm - minm)) * 100
    return df


def normalize(df, measure_type):
    if measure_type == "proc_meminfo":
        attribute = "dirty"
        df = normalization(df, attribute)
    return df


def get_network_table(measurement):
    key_idx = 0
    tag_list = ['systems', 'uuid', 'pkey', 'time_stamp', 'benchmark_tag', 'host', 'workload', 'iteration', 'file_name',
                'tag', 'is_workload_data', 'upload_date', 'lo']
    metric_columns = ['nics', 'rxbytes', 'rxpackets', 'rxerrs', 'rxdrop', 'rxfifo', 'rxframe', 'txbytes', 'txpackets',
                      'txerrs', 'txdrop', 'txfifo', 'txcolls', 'txcarrier']
    metric_list = []

    for nic in measurement.keys():
        if nic not in tag_list:
            metric_list.append([nic] + list(measurement.get(nic).values()))

    return pd.DataFrame(metric_list, columns=metric_columns)


def get_disk_table(measurement):
    key_idx = 0
    tag_list = ['systems', 'uuid', 'pkey', 'time_stamp', 'benchmark_tag', 'host', 'workload', 'iteration', 'file_name',
                'tag', 'is_workload_data', 'upload_date']
    metric_columns = ['disks', 'reads', 'rmerge', 'rkb', 'rmsec', 'writes', 'wmerge', 'wkb', 'wmsec', 'inflight',
                      'busy', 'backlog', 'xfers', 'bsize']
    metric_list = []

    for disk in measurement.keys():
        if disk not in tag_list:
            if not re.match(r'loop\d', disk) and not re.match(r'sr\d', disk):
                metric_list.append([disk] + list(measurement.get(disk).values()))

    return pd.DataFrame(metric_list, columns=metric_columns)


def process(json_dict, measure_type):
    try:
        i = 0
        z = len(json_dict)
        table_json_array = []

        while i < z:
            table_json = {}

            # =========== cpu utilization ===========
            if measure_type == "cpu_total":
                df_table = pd.DataFrame()

                metric_columns = ['username', 'nice', 'system_value', 'idle', 'iowait', 'irq', 'softirq', 'guest',
                                  'guestnice', 'steal']
                metric_list = [json_dict[i]['user'], json_dict[i]['nice'], json_dict[i]['sys'], json_dict[i]['idle'],
                               json_dict[i]['iowait'], json_dict[i]['hardirq'],
                               json_dict[i]['softirq'], json_dict[i]['guest'], json_dict[i]['guestnice'],
                               json_dict[i]['steal']]

                df_table = pd.DataFrame([metric_list], columns=metric_columns)

                util_calc = df_table['irq'][0] + df_table['nice'][0] + df_table['softirq'][0] + \
                            df_table['system_value'][0] + df_table['username'][0] + df_table['guest'][0] + \
                            df_table['guestnice'][0] + df_table['steal'][0]
                df_table['utilization'] = round(util_calc, 3)
                df_table['idle'] = round(100 - df_table['utilization'] - df_table['iowait'][0], 3)

                df_table['cpu'] = json_dict[i]['cpu']
                df_table['systems'] = json_dict[i]['systems']

                df_table['timestamp'] = json_dict[i]['time_stamp']
                df_table['last_idle'] = NULL
                df_table['last_total'] = NULL
                df_table['total'] = NULL

            # =========== memory utilization ===========
            elif measure_type == "proc_meminfo":
                df_table = pd.DataFrame()

                metric_columns = ['active', 'Active(anon)', 'Active(file)', 'anonhugepages', 'anonpages', 'bounce',
                                  'buffers', 'cached', 'commitlimit',
                                  'committed_as', 'directmap1g', 'directmap2m', 'directmap4k', 'dirty',
                                  'hardwarecorrupted', 'hugepages_free', 'hugepages_rsvd',
                                  'hugepages_surp', 'hugepages_total', 'hugepagesize', 'inactive', 'Inactive(file)',
                                  'Inactive(anon)', 'kernelstack', 'mapped',
                                  'memfree', 'memavailable', 'memtotal', 'mlocked', 'nfs_unstable', 'pagetables',
                                  'percpu', 'shmem', 'slab', 'sreclaimable',
                                  'sunreclaim', 'swapcached', 'swapfree', 'swaptotal', 'unevictable', 'vmallocchunk',
                                  'vmalloctotal', 'vmallocused', 'writeback',
                                  'writebacktmp', 'CmaFree', 'CmaTotal', 'FileHugePages', 'FilePmdMapped', 'Hugetlb',
                                  'KReclaimable', 'ShmemHugePages', 'ShmemPmdMapped']

                metric_list = [json_dict[i]['Active'], json_dict[i]['Active_anon'], json_dict[i]['Active_file'],
                               json_dict[i]['AnonHugePages'],
                               json_dict[i]['AnonPages'], json_dict[i]['Bounce'], json_dict[i]['Buffers'],
                               json_dict[i]['Cached'], json_dict[i]['CommitLimit'],
                               json_dict[i]['Committed_AS'], json_dict[i]['DirectMap1G'], json_dict[i]['DirectMap2M'],
                               json_dict[i]['DirectMap4k'], json_dict[i]['Dirty'],
                               json_dict[i]['HardwareCorrupted'], json_dict[i]['HugePages_Free'],
                               json_dict[i]['HugePages_Rsvd'], json_dict[i]['HugePages_Surp'],
                               json_dict[i]['HugePages_Total'], json_dict[i]['Hugepagesize'], json_dict[i]['Inactive'],
                               json_dict[i]['Inactive_file'], json_dict[i]['Inactive_anon'],
                               json_dict[i]['KernelStack'], json_dict[i]['Mapped'], json_dict[i]['MemFree'],
                               json_dict[i]['MemAvailable'], json_dict[i]['MemTotal'],
                               json_dict[i]['Mlocked'], json_dict[i]['NFS_Unstable'], json_dict[i]['PageTables'],
                               json_dict[i]['Percpu'], json_dict[i]['Shmem'],
                               json_dict[i]['Slab'], json_dict[i]['SReclaimable'], json_dict[i]['SUnreclaim'],
                               json_dict[i]['SwapCached'], json_dict[i]['SwapFree'],
                               json_dict[i]['SwapTotal'], json_dict[i]['Unevictable'], json_dict[i]['VmallocChunk'],
                               json_dict[i]['VmallocTotal'], json_dict[i]['VmallocUsed'],
                               json_dict[i]['Writeback'], json_dict[i]['WritebackTmp'], json_dict[i]['CmaFree'],
                               json_dict[i]['CmaTotal'], json_dict[i]['FileHugePages'],
                               json_dict[i]['FilePmdMapped'], json_dict[i]['Hugetlb'], json_dict[i]['KReclaimable'],
                               json_dict[i]['ShmemHugePages'], json_dict[i]['ShmemPmdMapped']]

                df_table = pd.DataFrame([metric_list], columns=metric_columns)

                df_table['utilization'] = (int(df_table['memtotal'][0]) - int(df_table['memavailable'][0])) / int(
                    df_table['memtotal'][0]) * 100
                cache_free = int(df_table['memfree'][0]) + int(df_table['buffers'][0]) + int(df_table['cached'][0])
                cache_used = int(df_table['memtotal'][0]) - (cache_free)
                df_table['cached_percent'] = (cache_used / (cache_used + cache_free)) * 100

                df_table['systems'] = json_dict[i]['systems']
                df_table['timestamp'] = json_dict[i]['time_stamp']

            # =========== network utilization ===========
            elif measure_type == "networks":
                df_table = pd.DataFrame()

                measurement = json_dict[i]
                df_table = get_network_table(measurement)

                df_table['delta_txbytes'] = df_table['txbytes']
                df_table['delta_rxbytes'] = df_table['rxbytes']
                df_table['systems'] = json_dict[i]['systems']

                df_table['txbytes'] = NULL
                df_table['rxbytes'] = NULL
                df_table['timestamp'] = json_dict[i]['time_stamp']
                df_table['rxmulticast'] = NULL
                df_table['rxcompressed'] = NULL
                df_table['txcompressed'] = NULL
                df_table['last_txbytes'] = NULL
                df_table['last_rxbytes'] = NULL
                df_table['delta_txbytes_norm'] = NULL
                df_table['delta_rxbytes_norm'] = NULL


            # =========== disk utilization ===========
            elif measure_type == "disks":
                df_table = pd.DataFrame()

                measurement = json_dict[i]
                df_table = get_disk_table(measurement)

                df_table['systems'] = json_dict[i]['systems']
                df_table['timestamp'] = json_dict[i]['time_stamp']

                df_table['last_weighted_time_spent_ios'] = NULL
                df_table['last_time_spent_ios'] = NULL
                df_table['time_spent_ios'] = NULL
                df_table['weighted_time_spent_ios'] = NULL

                if ((df_table['rmsec'][0] + df_table['wmsec'][0]) != 0):
                    df_table['weighted_percent'] = (df_table['rmerge'] + df_table['wmerge']) / (
                                df_table['rmsec'] + df_table['wmsec'])
                else:
                    df_table['weighted_percent'] = 0

                df_table['busy_percent'] = df_table['busy']
                df_table['busy'] = NULL

            df_table['pkey'] = json_dict[i]['pkey']
            df_table['uuid'] = json_dict[i]['uuid']
            df_table['benchmark_tag'] = json_dict[i]['benchmark_tag']
            df_table['host'] = json_dict[i]['host']
            df_table['workload'] = json_dict[i]['workload']
            df_table['iteration'] = json_dict[i]['iteration']
            df_table['is_workload_data'] = json_dict[i]['is_workload_data']

            df_table['time_stamp'] = json_dict[i]['time_stamp']
            df_table['upload_date'] = json_dict[i]['upload_date']

            df_table['tag'] = json_dict[i]['tag']

            table_json['data'] = df_table.to_dict('records')
            table_json_array.append(table_json)

            i = i + 1

        return table_json_array
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(" line number: {}, error : {}".format(exc_tb.tb_lineno, e))
        return 0


def drop_cols_from_df(df1, col_list):
    for col in col_list:
        if col in df1.columns:
            df1.drop(columns=col, axis=1, inplace=True)
        else:
            print("Not Found:", col)
    return df1


def restructure_for_sql(df, measure_type):
    try:
        # ==========filter the fixed attributes=======
        attr_common = ['tag', 'pkey', 'benchmark_tag', 'host', 'workload', 'iteration', 'time_stamp',
                       'is_workload_data']
        if measure_type == "cpu_total":
            dynamic_column = attr_common + ['cpu', 'systems']
            attr_list = ['timestamp', 'cpu', 'system_value', 'systems', 'last_idle', 'nice', 'irq', 'idle',
                         'last_total', 'utilization', 'iowait', 'username', 'total',
                         'softirq', 'upload_date', 'tag', 'benchmark_tag', 'host', 'workload', 'iteration',
                         'time_stamp', 'is_workload_data', 'guest', 'guestnice', 'steal', 'uuid', 'pkey']

        elif measure_type == "proc_meminfo":
            dynamic_column = attr_common + ['cached_percent', 'dirty_norm', 'systems']
            attr_list = ['timestamp', 'Inactive(anon)', 'unevictable', 'writebacktmp', 'memtotal', 'hugepages_surp',
                         'vmallocused', 'nfs_unstable',
                         'buffers', 'kernelstack', 'hugepages_rsvd', 'vmalloctotal', 'anonhugepages', 'systems',
                         'Inactive(file)', 'swaptotal', 'mapped', 'hugepagesize',
                         'memavailable', 'hardwarecorrupted', 'memfree', 'active', 'swapcached', 'directmap4k',
                         'utilization', 'commitlimit', 'shmem', 'sunreclaim',
                         'directmap2m', 'cached', 'hugepages_total', 'pagetables', 'swapfree', 'sreclaimable',
                         'inactive', 'vmallocchunk', 'percpu', 'Active(file)',
                         'mlocked', 'writeback', 'anonpages', 'hugepages_free', 'Active(anon)', 'directmap1g', 'slab',
                         'bounce', 'committed_as', 'dirty', 'upload_date',
                         'tag', 'benchmark_tag', 'host', 'workload', 'iteration', 'time_stamp', 'cached_percent',
                         'dirty_norm', 'is_workload_data',
                         'CmaFree', 'CmaTotal', 'FileHugePages', 'FilePmdMapped', 'Hugetlb', 'ShmemHugePages',
                         'ShmemPmdMapped', 'uuid', 'pkey']

        elif measure_type == "disks":
            dynamic_column = attr_common + ['disks', 'systems']
            attr_list = ['timestamp', 'disks', 'writes', 'busy_percent', 'reads', 'last_weighted_time_spent_ios',
                         'systems', 'last_time_spent_ios',
                         'time_spent_ios', 'weighted_time_spent_ios', 'weighted_percent', 'upload_date', 'tag',
                         'benchmark_tag', 'host', 'workload', 'iteration',
                         'time_stamp', 'is_workload_data', 'backlog', 'bsize', 'busy', 'inflight', 'rkb', 'rmerge',
                         'rmsec', 'wkb', 'wmerge', 'wmsec', 'xfers', 'uuid', 'pkey']

        elif measure_type == "networks":
            dynamic_column = attr_common + ['delta_txbytes', 'delta_rxbytes', 'systems', 'nics']
            attr_list = ['timestamp', 'nics', 'rxerrs', 'txpackets', 'rxmulticast', 'rxdrop', 'txbytes', 'rxpackets',
                         'systems', 'rxcompressed',
                         'txcompressed', 'txerrs', 'rxfifo', 'rxframe', 'txcarrier', 'txcolls', 'txdrop', 'txfifo',
                         'rxbytes', 'upload_date', 'tag',
                         'benchmark_tag', 'host', 'workload', 'iteration', 'time_stamp', 'last_txbytes', 'last_rxbytes',
                         'delta_txbytes', 'delta_rxbytes',
                         'delta_txbytes_norm', 'delta_rxbytes_norm', 'is_workload_data', 'uuid', 'pkey']

        # drop extra columns from json df
        extra_to_drop = set(df.columns) - set(attr_list)
        df = drop_cols_from_df(df, extra_to_drop)

        # get filtered df
        df = df[attr_list]
        # get dynamic df
        df_tmp = df[dynamic_column].copy()
        # remove from original df
        df = drop_cols_from_df(df, dynamic_column)
        # append df_tmp at the end of df columns
        # df_concat = pd.merge(df, df_tmp, on='common_column_name', how='outer')
        df_restuctured = pd.concat([df, df_tmp], axis=1)
        df_restuctured.rename(columns={'Unnamed: 0': 'rec_id'}, inplace=True)
        print(measure_type, ":", df_restuctured.columns)
        if len(df) == len(df_restuctured):
            return df_restuctured
        else:
            raise Exception("Error in restructuring")
    except Exception as e:
        print("Error in restructuring:", e)

def insertStatusTbl(filename, uuid_key, host, startTime, endTime, process_msg, status, status_percentage, userName,
                    orgID):
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


def update_uuid(endTime, host, status, process_msg, status_percentage, uuid, new_filename, TimeStamp):
    try:
        cur, engine, conn = connectToDB()
        query = """UPDATE upload_status SET end_date = %s , host = %s , status = %s , message = %s , status_percentage = %s , uuid = %s
                             WHERE file_name = %s and start_date = %s ; """
        cur.execute(query, (endTime, host, status, process_msg, status_percentage, uuid, new_filename, TimeStamp))
        print("updated progress into update status table. task status:", status_percentage)
        conn.commit()
    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")


def insertNativePlatformData(df_np, TimeStamp):
    try:
        cur, engine, conn = connectToDB()
        df_np.to_sql("nativeplatform_details", engine, index=False, if_exists="append")

        time.sleep(1)

        query = 'select "nativePlatformID" from nativeplatform_details where upload_time = %s'
        cur.execute(query, (TimeStamp,))
        uuid = cur.fetchone()

        return uuid
    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")


def getWorkloadReturnVal(host):
    cur, engine2, conn = connectToDB()
    tableList = ["cpu", "memory", "disk", "network"]
    tableCount = []
    for table in tableList:
        query = "SELECT count(*) FROM {} WHERE host = '{}';".format(table, host)
        data = []
        cur.execute(query, data)
        results = cur.fetchone()
        for r in results:
            print("Total number of rows uploaded in the {}:".format(table), r)

        tableCount.append(r)
    return tableCount


def ingestData(df_table, measure_type):
    cur, engine, conn = connectToDB()
    try:
        if measure_type == "cpu_total":
            df_table["time_stamp"] = pd.to_datetime(df_table["time_stamp"], utc=False)
            df_table["time_stamp"] = df_table["time_stamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

            df_table.to_sql("cpu", engine, index=False, if_exists="append")
            print("Data uploaded to cpu")

        elif measure_type == "proc_meminfo":
            df_table["time_stamp"] = pd.to_datetime(df_table["time_stamp"], utc=False)
            df_table["time_stamp"] = df_table["time_stamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

            df_table.to_sql("memory", engine, index=False, if_exists="append")
            print("Data uploaded to memory")

        elif measure_type == "networks":
            df_table["time_stamp"] = pd.to_datetime(df_table["time_stamp"], utc=False)
            df_table["time_stamp"] = df_table["time_stamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

            df_table.to_sql("network", engine, index=False, if_exists="append")
            print("Data uploaded to network")

        elif measure_type == "disks":
            df_table["time_stamp"] = pd.to_datetime(df_table["time_stamp"], utc=False)
            df_table["time_stamp"] = df_table["time_stamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

            df_table.to_sql("disk", engine, index=False, if_exists="append")
            print("Data uploaded to disk")

    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

    finally:
        cur.close()
        conn.close()

def update_ExceptionError(e, new_filename, start_date):
    try:
        endTime = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        process_msg = str(e)
        status = "Failed"
        status_percentage = "100%"
        updateStatusTbl(new_filename, endTime, process_msg, status, status_percentage, start_date)

    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
