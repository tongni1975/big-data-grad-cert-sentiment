from flask import Flask
from flask import request

app = Flask(__name__)

@app.route("/")
def home():
    return "Hello, World!"

@app.route("/ping")
def listen_agg_stat():
    stat_msg = request.args.get('msg')

    return '''<h1>{}</h1>'''.format(stat_msg)

if __name__ == "__main__":
    app.run(debug=True)