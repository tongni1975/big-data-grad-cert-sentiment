# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

WORKDIR /python-docker

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

# Install Java and set JAVA_HOME
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean && \
    mkdir -p /var/lib/apt/lists/partial && \
    rm -rf /var/lib/apt/lists/ && \
    rm -rf /var/cache/oracle-jdk8-installer;

# ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk/
# RUN export JAVA_HOME

CMD [ "python", "twitter_tweepy.py"]
CMD [ "python", "streamprocessor.py"]
CMD [ "python", "-m" , "flask", "run", "--host=0.0.0.0"]