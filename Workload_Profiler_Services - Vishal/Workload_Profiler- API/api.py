from utils import *

app = Flask(__name__)
base = connectDB()
cur = base.cursor()

@app.route('/get_hostname', methods=['GET'])
def postMark():
    try:
        hosts = []
        cur.execute("select host from cpu order by host")
        data = cur.fetchall()
        base.commit()
        set_1 = set(data)
        for i in set_1:
            hosts.append(i[0])
        resp_data = {"No. of hosts": len(hosts), "Total Hosts": hosts}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

@app.route('/getDisks', methods=['GET','POST'])
def diskPost():
    try:
        host = request.json["Host"]
        disks = []
        cur.execute("select distinct(disks) from disk where host=%s and disks not like 'loop%%' and disks not like 'dm%%' and disks not like 'sr%%'", [host])
        data = cur.fetchall()
        base.commit()
        for i in data:
            disks.append(i[0])
        resp_data = {"No. of Disks": len(disks), "Total Disks": disks}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

@app.route('/getNics', methods=['GET'])
def nicsPost():
    try:
        host = request.json["Host"]
        nics = []
        cur.execute("select distinct nics from network where host = %s and nics !='lo' order by nics", [host])
        data = cur.fetchall()
        base.commit()
        for i in data:
            nics.append(i[0])
        count = len(nics)
        resp_data = {"No. of nics": count, "Total nics": nics}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

@app.route('/dateRange', methods=['GET'])
def datePost():
    try:
        host = request.json["Host"]
        cur.execute("select date(min(timestamp)) as start_date,date(max(timestamp)) as End_date from cpu where host=%s", [host])
        data = cur.fetchall()
        base.commit()
        resp_data = {"Starting Date" : data[0][0], "End Date" : data[0][1]}
        response = json.dumps(resp_data, default = str)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

@app.route('/workloadComparison', methods=['POST'])
def fetchData1():
    try:
        Host = request.json['Host']
        Disk = request.json['Disk']
        nics = request.json['nics']
        percentile = request.json['percentile']
        start_date = request.json['start_date']
        end_date = request.json['end_date']
        create_anomaly_cpu(Host, start_date, end_date)
        data, column_name = create_anomaly_data_cpu(percentile, Host, Disk, nics)
        dict = responce(column_name, data)
        resp_data = {"Output": dict}
        response = json.dumps(resp_data, default = str)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

@app.route('/getPercentile', methods=['POST'])
def fetchData2():
    try:
        Host = request.json['Host']
        Disk = request.json['Disk']
        nics = request.json['nics']
        percentile1 = request.json['percentile']
        start_date = request.json['start_date']
        end_date = request.json['end_date']
        create_anomaly_cpu(Host, start_date, end_date)
        data, column_name = create_anomaly_data_cpu(percentile1, Host, Disk, nics)
        dict = responce(column_name, data)
        resp_data = {"CPU Utilization": dict}
        response = json.dumps(resp_data, default = str)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

@app.route('/percentile', methods=['POST'])
def perComparison():
    try:
        Host = request.json['Host']
        Disk = request.json['Disk']
        nics = request.json['nics']
        percentile1 = request.json['percentile1']
        percentile2 = request.json['percentile2']
        data1, column_name1 = create_anomaly_data_cpu(percentile1, Host, Disk, nics)
        data2, column_name2 = create_anomaly_data_cpu(percentile1, Host, Disk, nics)
        dict1 = responce(column_name1, data1)
        dict2 = responce(column_name2, data2)
        resp_data = {"Percentile1": dict1, "Percentile2" : dict2}
        response = json.dumps(resp_data, default = str)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
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

        for df in Data_focus:
            if df=="cpu":
                create_specific_cpu(Host, specific)
                data, column_name = create_anomaly_data_cpu(percentile, Host, Disk,nics)
                print(data)
                dict = responce(data, column_name)
            elif df=="Memory_utilization":
                create_specifi_Memory(Host, specific)
                data, column_name = create_anomaly_data_memory(percentile, Host, Disk,nics)
                dict=responce(data, column_name)
            elif df=="DiskBusy":
                create_specific_DiskBusy(Host, Disk, specific)
                data, column_name = create_anomaly_data_DiskBusy(percentile, Host, Disk,nics)
                dict=responce(data, column_name)
            elif df=="DiskWeighted":
                create_specific_DiskWeighted(Host, Disk, specific)
                data, column_name = create_anomaly_data_DiskWeighted(percentile, Host, Disk,nics)
                dict=responce(data, column_name)

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
        mycursor.execute("select  distinct(workload) from cpu where is_workload_data = 1 order by workload ")
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

@app.route('/benchmarkDisks', methods=['GET','POST'])
def diskPost1():
    try:
        workload = request.args.get('Workload')
        print(workload)
        disks = []
        cur.execute(
        "select distinct(disks) from disk where workload=%s and disks not like 'loop%%' and disks not like 'dm%%' and disks not like 'sr%%' order by disks", [workload])
        data = cur.fetchall()
        base.commit()
        for i in data:
            disks.append(i[0])
        count = len(disks)
        resp_data = {"No. of Disks": count, "Total Disks": disks}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

@app.route('/benchmarkNics', methods=['GET','POST'])
def diskPost2():
    try:
        workload = request.args.get('Workload')
        print(workload)
        disks = []
        cur.execute(
        "select distinct nics from network where workload = %s and nics !='lo' order by nics", [workload])
        data = cur.fetchall()
        base.commit()
        for i in data:
            disks.append(i[0])
        count = len(disks)
        resp_data = {"No. of nics": count, "Total nics": disks}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return  response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
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
        cpu_response=output_response(cpu_data, cpu_column_name,focus)
        dict.update({focus:cpu_response})
        print(type(dict))

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)