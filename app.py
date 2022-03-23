from flask import Flask
from flask import request, jsonify, redirect, render_template, url_for
from flask_sqlalchemy import SQLAlchemy
from pytz import timezone
from sympy import true
from datetime import datetime, timedelta

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db.sqlite'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class AlcDateTime(db.TypeDecorator):
    impl = db.DateTime
    
    def process_bind_param(self, value, dialect):
        if type(value) is str:
            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        return value

class Tweet(db.Model):
    author_id = db.Column(db.String(512))
    text = db.Column(db.String(512), primary_key=True)
    date = db.Column(AlcDateTime)
    sentiment_score = db.Column(db.Float())
    subjectivity = db.Column(db.Float())
    tone = db.Column(db.String(8))

@app.route("/")
def home():
    # past 1 hour tweets
    past_hour = datetime.now() - timedelta(hours= 1)
    #print("past hour {}".format(past_hour))
    tweets = Tweet.query.filter(Tweet.date >= past_hour).all()
    return render_template("base.html", tweetList=tweets)

@app.route("/ping", methods=['POST', 'GET'])
def listen_agg_stat():
    stat_msg = request.get_json()
    print(stat_msg)

    latest_tweet = Tweet(**stat_msg)
    # no primary key
    db.session.add(latest_tweet)
    db.session.commit()

    return redirect(url_for("home"))
    

if __name__ == "__main__":
    db.create_all()
    app.run(debug=True)