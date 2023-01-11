import json
from connect2db import *
from flask import Flask, request, jsonify, Response
from query_function import *
from output_response import *
from flask_cors import CORS
from werkzeug.utils import secure_filename
import csv
import os
from datetime import datetime
from collections import defaultdict
from collections import OrderedDict
import json as JSON
#from diskall import *


app = Flask(__name__)
CORS(app)
base = connect_db()
mycursor = base.cursor()

@app.route('/get_hostname')
def host_name():
    try:
        l = []
        #mycursor.execute("select host from cpu order by host")
        mycursor.execute("select distinct(host) from network where host in (select distinct(host) from disk)")
        data=mycursor.fetchall()
        for i in data:
            if i[0] not in l:
                l.append(i[0])
        resp_data = {"message": "successful!", "host": l}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return response
    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

@app.route('/disk',methods =['GET'])
def disk_name():
    try:
        host = request.args.get('host')
        l = []
        #mycursor.execute("select disks from disk where host=%s", (host,))
        mycursor.execute(''' select disks from disk where host=%s and disks not like 'loop%%' and disks not like 'dm%%' and disks not like 'sr%%' ''', (host,))
        data=mycursor.fetchall()
        for i in data:
            if i[0] not in l:
                l.append(i[0])
        resp_data = {"message": "successful!", "disk": l}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

@app.route('/nics',methods =['GET'])
def nicks_name():
    try:
        host = request.args.get('host')
        l = []
        Diskquery = ("select nics from network where host=%s")
        mycursor.execute(Diskquery, (host,))
        data = mycursor.fetchall()
        for i in data:
            if i[0] not in l:
                l.append(i[0])

        resp_data = {"message": "successful!", "nicks": l}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return response

    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

@app.route('/start_end',methods =['GET'])
def start_endtime():
    try:
        host = request.args.get('host')
        l=[]
        #cur.execute("select host,date(min(timestamp)) as start_date,date(max(timestamp)) as End_date from cpu group by host;")
        mycursor.execute("select min(timestamp) as start_date,max(timestamp) as End_date from cpu where host=%s;",(host,))
        data=mycursor.fetchall()
        for i in data:
            l.extend([str(i[0]),str(i[1])])

        resp_data = {"Timestamp":l}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

@app.route('/workload_comparison', methods=['POST'])
def workload_comparison():
    try:
        Host = request.json['Host']
        Disk = request.json['Disk']
        nics = request.json['nics']
        percentile = request.json['percentile']
        start_date = request.json['start_date']
        end_date = request.json['end_date']
        Data_focus= request.json['Data_focus']

        dict = {}
        for i in Data_focus:
            if i=="Cpu_utilization":
                create_anomaly_cpu(Host, start_date, end_date)
                data, column_name = create_anomaly_data_cpu(percentile, Host, Disk, nics)
                cpu=output_response(data, column_name,i)
                dict.update({"Cpu_utilization":cpu})
            elif i=="Memory_utilization":
                create_anomaly_Memory(Host, start_date, end_date)
                data, column_name = create_anomaly_data_memory(percentile, Host, Disk,nics)
                memory=output_response(data, column_name,i)
                dict.update({"Memory_utilization":memory})
            elif i=="DiskBusy":
                create_anomaly_DiskBusy(Host, Disk, start_date, end_date)
                data, column_name = create_anomaly_data_DiskBusy(percentile, Host, Disk,nics)
                DiskBusy=output_response(data, column_name,i)
                dict.update({"DiskBusy":DiskBusy})
            elif i=="DiskWeighted":
                create_anomaly_DiskWeighted(Host, Disk, start_date, end_date)
                data, column_name = create_anomaly_data_DiskWeighted(percentile, Host, Disk,nics)
                DiskWeighted=output_response(data, column_name,i)
                dict.update({"DiskWeighted":DiskWeighted})

        resp_data = {"Output": dict}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

@app.route('/percentile_comparison', methods=['POST'])
def percentile_comparison(): #  comparision between 2 percentile for given host disk and nics
    try:
        Host = request.json['Host']
        Disk = request.json['Disk']
        nics = request.json['nics']
        percentile1 = float(request.json['percentile1'])
        percentile2 = float(request.json['percentile2'])
        Data_focus= request.json['Data_focus']

        if Data_focus=="Cpu_utilization":
            #create_anomaly_cpu(Host, start_date, end_date)
            data1, column_name1 = create_anomaly_data_cpu(percentile1, Host, Disk, nics)
            data2, column_name2 = create_anomaly_data_cpu(percentile2, Host, Disk, nics)

        elif Data_focus=="Memory_utilization":
            #create_anomaly_Memory(Host, start_date, end_date)
            data1, column_name1 = create_anomaly_data_memory(percentile1, Host, Disk, nics)
            data2, column_name2 = create_anomaly_data_memory(percentile2, Host, Disk,nics)

        elif Data_focus=="DiskBusy":
            #create_anomaly_DiskBusy(Host, Disk, start_date, end_date)
            data1, column_name1 = create_anomaly_data_DiskBusy(percentile1, Host, Disk, nics)
            data2, column_name2 = create_anomaly_data_DiskBusy(percentile2, Host, Disk,nics)

        elif Data_focus=="DiskWeighted":
            #create_anomaly_DiskWeighted(Host, Disk, start_date, end_date)
            data1, column_name1 = create_anomaly_data_DiskWeighted(percentile1, Host, Disk, nics)
            data2, column_name2 = create_anomaly_data_DiskWeighted(percentile2, Host, Disk,nics)

        resp1 = output_response(data1, column_name1, Data_focus)
        resp2 = output_response(data2, column_name2, Data_focus)
        resp_data = {"percentile1":resp1, "percentile2":resp2}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return  response
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

@app.route('/workload',methods =['GET'])
def workload():#get bench marks from database table
    try:
        l = []
        #mycursor.execute("select  distinct(workload) from cpu where is_workload_data = 1 order by workload ")
        mycursor.execute("select distinct(workload) from network where workload in (select distinct(workload) from disk)")
        data=mycursor.fetchall()
        #print(data)
        for i in data:
            if i[0] not in l:
                l.append(i[0])
        resp_data = {"message": "successful!", "workload": l}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

@app.route('/workload_disk',methods =['GET'])
def get_disk_workload(): # get bench mark disks from database table
    try:
        l = []
        workload = request.args.get('workload') #spec_cpu_t2d_2vcpu8g_mm
        mycursor.execute('''select  distinct(disks) from disk where workload=%s and disks not like 'loop%%' and disks not like 'dm%%' and disks not like 'sr%%' order by disks''',(workload,))
        data = mycursor.fetchall()
        for i in data:
            l.append(i[0])
        resp_data = {"message": "successful!", "disk": l}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return response
    except Exception as e:
        error = {"error" : "Connection with database is failed"}
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        print(f"\n{'=' * 30}\n{error}\n{'=' * 30}\n")
        return jsonify(error)

@app.route('/workload_nics',methods =['GET'])
def get_nics_workload(): # get bench mark nics from database table
    try:
        l = []
        workload = request.args.get('workload') #spec_cpu_t2d_2vcpu8g_mm
        mycursor.execute('''select distinct(nics)  from network where workload = %s and nics !='lo' order by nics''',(workload,))
        data = mycursor.fetchall()
        for i in data:
            l.append(i[0])
        resp_data = {"message": "successful!", "nics": l}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return response
    except Exception as e:
        error = {"error" : "Connection with database is failed"}
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        print(f"\n{'=' * 30}\n{error}\n{'=' * 30}\n")
        return jsonify(error)

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
        focus = "Memory_utilization"
        memory_data, memory_column_name = create_wl_data_memory(percentile, workload, Disk, nics)
        memory_response = output_response(memory_data, memory_column_name,focus)
        dict.update({focus: memory_response})

        create_wl_DiskBusy(workload,Disk)
        focus = "DiskBusy"
        DiskBusy_data, DiskBusy_column_name = create_wl_data_DiskBusy(percentile, workload, Disk, nics)
        DiskBusy_response = output_response(DiskBusy_data, DiskBusy_column_name,focus)
        dict.update({focus: DiskBusy_response})

        create_wl_DiskWeighted(workload,Disk)
        focus = "DiskWeighted"
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
    

ALLOWED_EXTENSIONS = set(['gz'])


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def chekIfHostExists(host):
    #cur, engine2, conn = connectToDB()
    
    query = "SELECT host FROM upload_status WHERE host = '{}';".format(host)
    mycursor.execute(query)
    results = mycursor.fetchone()
    if results is None:
        return 1
    else:
        for result in results:
            return -1




@app.route('/upload', methods=['POST'])
def upload():
    try:
        if request.method == 'POST':
            file = request.files.get('file')
            host = request.form['host']
            userName = request.form['userName']
            orgID = request.form['orgID']

            filename = secure_filename(file.filename)
            
            server_time = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            server_time_cleaned = server_time.replace("-", "").replace(" ", "_").replace(":", "")
            new_filename = filename.split("_")[0] + "_" + filename.split("_")[1] + "_" + server_time_cleaned

            validate_host = chekIfHostExists(host)
            if(validate_host == 1):
                try:
                    start_date = server_time
                    endTime = None
                    process_msg = "File started coping to server"
                    status = "Started"
                    status_percentage = "0%"
                    uuid_key = None
                    insertStatusTbl(new_filename, uuid_key, host, start_date, endTime, process_msg, status, status_percentage, userName, orgID)

                    if file and allowed_file(file.filename):

                        #current_directory = os.path.normpath(os.getcwd() + os.sep + os.pardir)
                        current_directory="./"
                        final_directory = os.path.join(current_directory, "uploaded_files")
                        if not os.path.isdir(final_directory):
                            os.makedirs(final_directory)

                        files_location = os.path.join(final_directory, filename)
                        file.save(files_location)
                        process_msg = "File copied to server"
                        status = "In progress"
                        status_percentage = "20%"
                        updateStatusTbl(new_filename, endTime, process_msg, status, status_percentage, start_date)

                        return jsonify({"Message" : "File copied to server successfully!!", "status" : 1, "start_date" : server_time})

                    else:
                        process_msg = "Invalid file extension!"
                        status = "Failed"
                        status_percentage = "100%"
                        updateStatusTbl(new_filename, endTime, process_msg, status, status_percentage, start_date)
                        return jsonify({"Message": "Invalid file extension!"})

                except Exception as e:
                    update_ExceptionError(e, new_filename, start_date = server_time)
                    print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
                    error = {"error": str(e)}
                    return jsonify(error)
            
            else:
                return jsonify({"Message": "Host already exists in the database"})


    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

@app.route('/upload_status', methods=["GET"])
def host_names():
    try:
        #cur, engine, conn = connectToDB()
        base.autocommit = True
        if request.method == 'GET':
            mycursor.execute('select file_name, host as native_workload, start_date, end_date, status, status_percentage, message, username, org_id from upload_status order by start_date desc limit 100;')
            data = mycursor.fetchall()
            column = [desc[0] for desc in mycursor.description]
            reponse_data = []
            data_respose = {}
            for i in data:
                one_resp_data = {}
                len_column = len(column)
                for j in range(0,len_column):
                    one_resp_data.update({column[j]:str(i[j])})
                reponse_data.append(one_resp_data)
            data_respose.update({"reponse_data":reponse_data})
            key_order = ["file_name", "username", "org_id", "native_workload", "start_date","end_date","status","status_percentage","message"]
            result = defaultdict(list)
            for dic in data_respose["reponse_data"]:
                ordered = OrderedDict((key, dic.get(key)) for key in key_order)
                result["reponse_data"].append(ordered)
            response = JSON.dumps(result)
            response = Response(response, status=200, mimetype='application/json')
            return response

    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)




if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
