from flask import Flask, request, jsonify, Response

def output_response(data, column_name):
    try:
        resp_dict = {}
        index_len = len(column_name)   #=11
        for index in range(0, index_len): #(0,11)
            if data[0][index] is None:
                resp_dict.update({column_name[index]: 0})
            else:
                if data[0][index]==data[0][1] or data[0][index]==data[0][8] :
                    resp_dict.update({column_name[index]: str(data[0][index])})
                elif data[0][index]==data[0][0]:
                    resp_dict.update({column_name[index]: data[0][index]})
                else:
                    resp_dict.update({column_name[index]: float(data[0][index])})
        return resp_dict

    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)

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
                    resp_dict.update({column_name[index]: float(data[0][index])})
        return resp_dict

    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)