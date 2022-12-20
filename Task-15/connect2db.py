import psycopg2
from flask import Flask, request, jsonify, Response

def connectDB():
    filename = "db.config"
    contents = open(filename).read()
    config = eval(contents)

    try:
        dbase = psycopg2.connect(
            host = config["host"],
            dbname = config["dbname"],
            user = config["user"],
            password = config["password"],
            port = config["port"]
        )
        return dbase

    except Exception as e:
        error = {"error" : "Connection with databse is failed."}
        print(f"\n{'='*30}\n{e}\n{'='*30}\n")
        print(f"\n{'='*30}\n{error}\n{'='*30}\n")
        # error = {"error": str(e)}
        return jsonify(error)  

if (__name__ == "__main__"):
   connectDB()  