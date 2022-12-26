from flask import Flask
import psycopg2

app = Flask(__name__)

conn= psycopg2.connect(host="localhost",port=5432,user="postgres",password="postgres",database='infobell')

cur=conn.cursor()
cur.execute("CREATE TABLE if not exists Subject(Sub_ID INT, Sub_Name varchar(20));")
cur.execute("Insert into Subject Values (1,'English'),(2,'French'),(2,'Science'),(2,'Maths');")
cur.execute("SELECT * FROM Subject")
rows = cur.fetchall()
#for row in rows:
 #   print(row)

# Close communications with database
cur.close()
conn.close()


#@app.route('/')
@app.route('/home')
def home():
    return rows


if __name__=='__main__':

    app.run(host='0.0.0.0',port=5000, debug=True)