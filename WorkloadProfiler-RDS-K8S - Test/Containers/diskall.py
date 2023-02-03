import pandas as pd
import os
import glob
import psycopg2
import psycopg2.extras as extras
import warnings
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote
from flask import jsonify
import time
from datetime import datetime


warnings.filterwarnings("ignore")


def connectToDB():
    try:
        conn = psycopg2.connect(
            database="services_api",
            user="postgres",
            password="postgres",
            #host="localhost",
            host="database-1.cnahwufv56cn.us-east-1.rds.amazonaws.com",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        #engine = create_engine("postgresql+psycopg2://postgres:%s@localhost:5432/etl" % urlquote("postgres"))
        engine = create_engine("postgresql+psycopg2://postgres:%s@database-1.cnahwufv56cn.us-east-1.rds.amazonaws.com:5432/services_api" % urlquote("postgres"))
        return cur, engine, conn
    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)



def insertStatusTbl(filename, uuid_key, host, startTime, endTime, process_msg, status, status_percentage, userName, orgID):
    try:
        cur, engine, conn = connectToDB()
        uuid_key = None
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