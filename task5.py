from utils import *

app = Flask(__name__)
base = connectDB()
cur = base.cursor()

@app.route('/marks')
def unique():
    l = []
    cur.execute("select Total_marks from marks")
    data = cur.fetchall()
    for i in data:
        if i[0] not in l:
            l.append(i[0])
    resp_data = {"message": "successful!","Unique marks":l}
    response = json.dumps(resp_data)
    response = Response(response, status=200, mimetype='application/json')
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

