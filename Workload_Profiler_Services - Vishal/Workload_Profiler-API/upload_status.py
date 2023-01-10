from utils import *

#warnings.filterwarnings("ignore")

# Upload API
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
def connectToDB():
    try:
        conn = psycopg2.connect(
            database="Services",
            user="postgres",
            password="postgress",
            host="localhost",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        engine = create_engine("postgresql+psycopg2://postgres:%s@localhost:5432/Services" % urlquote("postgress"))
        return cur, engine, conn
    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)


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