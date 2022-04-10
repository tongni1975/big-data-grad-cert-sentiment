from datetime import datetime
import sqlite3
#from flask_sqlalchemy import SQLAlchemy


sqlite_file = 'prediction.sqlite'


# class Price(db.Model):
#     author_id = db.Column(db.String(512), primary_key=True)
#     text = db.Column(db.String(512), primary_key=True)
#     date = db.Column(AlcDateTime)


def persist(price, price_, typ, time):
    connection = sqlite3.connect(sqlite_file)
    cursor = connection.cursor()

    values = [price, price_, typ, time]
    sql = 'INSERT INTO tweets(cur_price, pred_price, type, date) VALUES(?, ?, ?, ?)'

    print(sql)

    cursor.execute(sql, values)
    connection.commit()
    connection.close()


def get_last_price(typ):
    connection = sqlite3.connect(sqlite_file)
    cursor = connection.cursor()

    sql = 'select * from Prediction limit 1'

    print(sql)

    cursor.execute(sql)
    connection.commit()
    connection.close()


def open():
    # TODO
    print("opending db connection")

    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    # D - day, H - hour, W - week
    conn.execute(
        "CREATE TABLE IF NOT EXISTS Prediction (cur_price Text, pred_price TEXT, type TEXT CHECK(type IN ('H','D','W') )  , date DATETIME)")
    # todo  create other 2 tables
    conn.commit()

    # return conn
    conn.close()


open()
