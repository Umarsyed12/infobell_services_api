import psycopg2
from flask import Flask
app = Flask(__name__)
conn = psycopg2.connect(database="Practice",host="localhost",user="postgres",password="postgress",port="5432")
cursor = conn.cursor()

app = Flask(__name__)
@app.route('/sample')
def test():
   cursor.execute("Create table if not exists Sample (Name varchar(20),emp_id integer,dept_name varchar(20),salary integer)")
   cursor.execute ("Insert into Sample (Name,emp_id,dept_name,salary)values('Akshad',11,'Hr',20000)")
   cursor.execute("select * from sample")
   result = cursor.fetchall()
   return result

test()
conn.commit()

if __name__ == '__main__':
   app.run('127.0.0.1', port=5500)