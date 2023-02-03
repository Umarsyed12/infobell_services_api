import psycopg2
from flask import jsonify
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote


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


if __name__ == '__main__':
    connect_db()
