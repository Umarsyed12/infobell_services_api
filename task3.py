from utils import *

app = Flask(__name__)
conn = connectDB()
cur = conn.cursor()

def create_table(header):
    a,b,c = header[0],header[1],header[2]
    cur.execute(f"CREATE TABLE if not exists marks ({a} integer, {b} varchar(20), {c} integer);")
    conn.commit()
    response = print('Successfully created a table')
    return response

def insert_table(v):
    global a,b,c,d
    for i in range(len(v)):
        a,b,c = v[0],v[1],v[2]
        d = (int(c)*10)/100
    l = (a,b,c,d)
    cur.execute(f"Insert into marks Values (%s,%s,%s,%s)",l)
    response = print('Successfully inserted a record')
    return response

@app.route('/task',methods=["GET"])
def api():
    try:
        with open('csv_demo.csv') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            header = next(csv_reader)
            create_table(tuple(header))
            rec = []
            if header != None:
                for row in csv_reader:  # row variable is a list that represents a row in csv
                    rec.append(row)
                    insert_table(row)
            conn.commit()
        resp_data = {"message": "successful!"}
        response = json.dumps(resp_data)
        response = Response(response, status=200, mimetype='application/json')
        return response
    except Exception as e:
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        error = {"error": str(e)}
        return jsonify(error)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)