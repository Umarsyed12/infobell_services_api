from flask import Flask, request, jsonify, Response

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
                    resp_dict.update({column_name[index]: round(float(data[0][index]),2)})
        return resp_dict

    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

    
    
def output_response2(data, column_name,focus,max): #for benchmark comparison only
    try:
        resp_dict = {}
        resp_dict.update({"data_focus": focus})
        print(type(max))
        print()
        diff = round(float(data[0][2]) - max,2)
        for i in range(4):
            if data[0][i] == data[0][1] :
                resp_dict.update({column_name[i]: str(data[0][i])})
            elif data[0][i] == data[0][0]:
                resp_dict.update({column_name[i]: data[0][i]})
            else:
                resp_dict.update({column_name[i]: round(float(data[0][i]),2)})
        resp_dict.update({"host": data[0][-1]})
        return [resp_dict], diff

    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)


def max_value(dict):
    try:
        cpu_utilization=[]
        memory_utilization=[]
        disk_busy=[]
        disk_weighted=[]

        for key in dict:
            cpu_utilization.append(dict[key]["cpu_utilization"])
            memory_utilization.append(dict[key]["memory_utilization"])
            disk_busy.append(dict[key]["disk_busy"])
            disk_weighted.append(dict[key]["disk_weighted"])
        resp_dict={}
        resp_dict.update({"cpu_utilization_max": round(max(cpu_utilization),2)})
        resp_dict.update({"memory_utilization_max": round(max(memory_utilization),2)})
        resp_dict.update({"disk_busy_max": round(max(disk_busy),2)})
        resp_dict.update({"disk_weighted_max": round(max(disk_weighted),2)})
        return resp_dict

    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)
