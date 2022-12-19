from utils import *

app = Flask(__name__)
base = connectDB()
cur = base.cursor()

@app.route('/',methods =['GET'])
def start_endtime():
    l={}
    cur.execute("select host,date(min(timestamp)) as start_date,date(max(timestamp)) as End_date from cpu group by host;")
    data = cur.fetchall()
    for i in data:
        l.update({str(i[0]):[str(i[1]),str(i[2])]})

    resp_data = {"Timestamp":l}
    response = json.dumps(resp_data)
    response = Response(response, status=200, mimetype='application/json')
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)