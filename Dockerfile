# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

ENV TZ="Asia/Singapore"

WORKDIR /python-docker

COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt


# Install Java and set JAVA_HOME
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    #    apt-get install -y ant && \
    apt-get clean && \
    mkdir -p /var/lib/apt/lists/partial && \
    rm -rf /var/lib/apt/lists/ && \
    rm -rf /var/cache/oracle-jdk8-installer;

# ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk/
# RUN export JAVA_HOME

#CMD [ "python", "-m" , "flask", "run", "--host=0.0.0.0"]

COPY . .

RUN chmod a+x run.sh

CMD ["./run.sh"]