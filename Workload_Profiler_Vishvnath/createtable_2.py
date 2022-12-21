
import psycopg2
conn= psycopg2.connect(host="localhost",port=5432,user="postgres",password="postgres",database='project')
print("connected")
cur=conn.cursor()
cur.execute("CREATE TABLE if not exists Subject1 (Sub_ID INT, Sub_Name varchar(20));")
cur.execute("Insert into Subject1 Values (1,'English'),(2,'French'),(3,'Science'),(4,'Maths');")
cur.execute("SELECT * FROM Subject1")



rows = cur.fetchall()
for row in rows:
   print(row)

conn.commit()
print("Successfully")


# Close communications with database
cur.close()
conn.close()