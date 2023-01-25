import psycopg2
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote
from flask import jsonify

def connect_db():
    filename = "db.config"
    content = open(filename).read()
    config = eval(content)

    try:
        dbase = psycopg2.connect(
            host =config["host"],
            database=config["dbname"],
            user=config["user"],
            password= config["password"],
            port=config["port"]
        )
        return dbase

    except Exception as e:
        error = {"error" : "Connection with database is failed"}
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        print(f"\n{'=' * 30}\n{error}\n{'=' * 30}\n")
        return jsonify(error)

def connectToDB():
    try:
        conn = psycopg2.connect(
            database="etl",
            user="postgres",
            password="postgres",
            host="host.docker.internal",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        engine = create_engine("postgresql+psycopg2://postgres:postgres@host.docker.internal:5432/etl")
        return cur, engine, conn
    except Exception as e:
        print(f"\n{'=' * 30}\n{e}\n{'=' * 30}\n")
        error = {"error": str(e)}
        return jsonify(error)


if __name__ == '__main__':
    connect_db()
