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
    scaler = joblib.load('model/scaler_.save')
    data = scaler.transform(data)
    data = np.reshape(data, (data.shape[0], data.shape[1], 1))

    import pickle
    with open('model/model_hour_.pickle', 'rb') as file:
        model = pickle.load(file)

    yhat = model.predict(data)

    yhat_inverse = scaler.inverse_transform(yhat.reshape(-1, 1))
    return yhat_inverse


def send_price(c, p):
    # dump json
    price_pair = {}
    # float32 cant be serialized -> float64
    price_pair['current'] = np.float64(c)
    price_pair['predict'] = np.float64(p)

    #out = json.dumps(price_pair)

    # print(out)
    requests.post(ticker_url, json=price_pair)


def get_next_hour_bitcoin_price():
    res = requests.get(bitcoin_price_url)
    cur_price = float(res.json().get("USD"))
    pred_price = predict_nexthour(cur_price)[0][0]
    #print("cur_price: {} pred_price: {}".format(cur_price, pred_price))

    send_price(cur_price, pred_price)

# save predicted values


def persist(x, typ):
    # hour price

    # day price
    print("todo")


schedule.every(1).hour.do(get_next_hour_bitcoin_price)
while True:
    schedule.run_pending()
    schedule.run_all(delay_seconds=10)

    time.sleep(1)
