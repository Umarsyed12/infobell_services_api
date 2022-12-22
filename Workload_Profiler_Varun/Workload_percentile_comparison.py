import json
from Workload_Connectdb import *
from flask import Flask, request, jsonify, Response
from Workload_Comparison import *

app = Flask(__name__)
base = connect_db()
mycursor = base.cursor()


@app.route('/percentile', methods=['POST'])
def percentile_comparison(): #  comparision between 2 percentile for given host disk and nics
    try:
        Host = request.json['Host']
        Disk = request.json['Disk']
        nics = request.json['nics']
        percentile1 = float(request.json['percentile1'])
        percentile2 = float(request.json['percentile2'])
        start_date = request.json['start_date']
        end_date = request.json['end_date']
        Data_focus= request.json['Data_focus']

        if Data_focus=="Cpu_utilization":
            create_anomaly_cpu(Host, start_date, end_date)
            data1, column_name1 = create_anomaly_data_cpu(percentile1, Host, Disk, nics)
            data2, column_name2 = create_anomaly_data_cpu(percentile2, Host, Disk, nics)

        elif Data_focus=="Memory_utilization":
            create_anomaly_Memory(Host, start_date, end_date)
            data1, column_name1 = create_anomaly_data_memory(percentile1, Host, Disk, nics)
            data2, column_name2 = create_anomaly_data_memory(percentile2, Host, Disk,nics)

        elif Data_focus=="DiskBusy":
            create_anomaly_DiskBusy(Host, Disk, start_date, end_date)
            data1, column_name1 = create_anomaly_data_DiskBusy(percentile1, Host, Disk, nics)
            data2, column_name2 = create_anomaly_data_DiskBusy(percentile2, Host, Disk,nics)

        elif Data_focus=="DiskWeighted":
            create_anomaly_DiskWeighted(Host, Disk, start_date, end_date)
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)