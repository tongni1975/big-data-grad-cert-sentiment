import requests
import schedule
import time
import numpy as np
import joblib
import json


bitcoin_price_url = "https://min-api.cryptocompare.com/data/price?fsym=BTC&tsyms=USD"
ticker_url = "http://127.0.0.1:5000/ticker"

#  predict next hour


def predict_nexthour(data):
    data = np.float64(data)
    values = data.reshape(-1, 1)
    data = values.astype('float32')
    #from sklearn.externals import joblib
    scaler = joblib.load('model/scaler_.save')
    data = scaler.transform(data)
    data = np.reshape(data, (data.shape[0], data.shape[1], 1))

    import pickle
    with open('model/model_hour_.pickle', 'rb') as file:
        model = pickle.load(file)

    yhat = model.predict(data)

    yhat_inverse = scaler.inverse_transform(yhat.reshape(-1, 1))
    return yhat_inverse


def predict_nextday(data):
    data = np.float64(data)
    values = data.reshape(-1, 1)
    data = values.astype('float32')
    #from sklearn.externals import joblib
    scaler = joblib.load('model/scaler_daily.save')
    data = scaler.transform(data)
    data = np.reshape(data, (data.shape[0], data.shape[1], 1))

    import pickle
    with open('model/model_daily.pickle', 'rb') as file:
        model = pickle.load(file)

    yhat = model.predict(data)

    yhat_inverse = scaler.inverse_transform(yhat.reshape(-1, 1))
    return yhat_inverse


def get_next_hour_bitcoin_price(c):
    pred_price = predict_nexthour(c)[0][0]
    #print("cur_price: {} pred_price: {}".format(cur_price, pred_price))

    return pred_price


def get_next_day_bitcoin_price(c):
    pred_price = predict_nextday(c)[0][0]

    return pred_price


def send_price():
    res = requests.get(bitcoin_price_url)
    cur_price = float(res.json().get("USD"))
    pred_price_h = get_next_hour_bitcoin_price(cur_price)
    pred_price_d = get_next_day_bitcoin_price(cur_price)

    # dump json
    price_pair = {}

    # float32 cant be serialized -> float64
    price_pair['current'] = np.float64(cur_price)
    price_pair['predict_h'] = np.float64(pred_price_h)
    price_pair['predict_d'] = np.float64(pred_price_d)
    #out = json.dumps(price_pair)

    # print(out)
    requests.post(ticker_url, json=price_pair)


schedule.every(1).hours.at(":00").do(send_price)
while True:
    schedule.run_pending()
    time.sleep(1)
