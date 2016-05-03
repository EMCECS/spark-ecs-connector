FROM debian:jessie

RUN echo "deb http://dl.bintray.com/sbt/debian /" > /etc/apt/sources.list.d/sbt.list \
  && apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823 \
  && apt-get update \
  && apt-get install -y curl wget unzip openjdk-7-jdk sbt

RUN \
  curl -sL --retry 3 "http://mirror.tcpdiag.net/apache/incubator/zeppelin/0.5.6-incubating/zeppelin-0.5.6-incubating-bin-all.tgz" \
  | gunzip \
  | tar x -C /opt/

ENV ZEPPELIN_HOME /opt/zeppelin-0.5.6-incubating-bin-all

WORKDIR /root
RUN git clone https://github.com/emcvipr/spark-ecs-s3.git \
  && cd spark-ecs-s3
  && sbt publishM2
  
EXPOSE 8080

ENV SPARK_LOCAL_IP 127.0.0.1

ADD notebook.tar $ZEPPELIN_HOME

WORKDIR $ZEPPELIN_HOME
CMD bin/zeppelin.sh


