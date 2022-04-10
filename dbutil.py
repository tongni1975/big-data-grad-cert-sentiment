from datetime import datetime
import sqlite3
import json
#from flask_sqlalchemy import SQLAlchemy


sqlite_file = 'prediction.sqlite'


# class Price(db.Model):
#     author_id = db.Column(db.String(512), primary_key=True)
#     text = db.Column(db.String(512), primary_key=True)
#     date = db.Column(AlcDateTime)


def persist(price, price_h, price_d, price_w, time):
    connection = sqlite3.connect(sqlite_file)
    cursor = connection.cursor()

    values = [price, price_h, price_d, price_w, time]
    sql = 'INSERT INTO Prediction (cur_price, pred_price_hour, pred_price_day, pred_price_week, date) VALUES(?, ?, ?, ?, ?)'

    cursor.execute(sql, values)
    connection.commit()
    connection.close()


def get_last_price():
    connection = sqlite3.connect(sqlite_file)
    cursor = connection.cursor()

    sql = 'select * from Prediction order by date desc limit 1'

    cursor.execute(sql)

    data = cursor.fetchone()
    keys = ["current", "predict_hour", "predict_day", "predict_week", "date"]

    #json_data = []
    #json_data.append(dict(zip(keys, data)))

    connection.commit()
    connection.close()

    return json.dumps(dict(zip(keys, data)))


def open():
    # TODO
    print("opending db connection")

    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    # D - day, H - hour, W - week
    conn.execute(
        "CREATE TABLE IF NOT EXISTS Prediction (cur_price Text, pred_price_hour TEXT, pred_price_day TEXT, pred_price_week TEXT, date DATETIME)")
    # todo  create other 2 tables
    conn.commit()

    # return conn
    conn.close()


open()
