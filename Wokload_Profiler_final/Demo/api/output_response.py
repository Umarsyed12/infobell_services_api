from flask import jsonify

def responce(column_name, data):
    try:
        dict = {}
        a=column_name
        b=data
        j, k = 0, 0
        for i in range(len(b)):
            if i==12:
                break
            else:
                dict[a[j]] = b[i][k]
                j+=1
                k+=1
        for i in a:
            if dict[i] is None:
                dict[i] = 0
        return dict

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