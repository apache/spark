# Spark 0.8.1
#
FROM ubuntu:precise

RUN echo "deb http://archive.ubuntu.com/ubuntu precise main universe" > /etc/apt/sources.list

# Upgrade package index
RUN apt-get update

# install a few other useful packages plus Open Jdk 7
RUN apt-get install -y less openjdk-7-jre-headless net-tools vim-tiny sudo openssh-server

ENV SCALA_VERSION 2.9.3
ENV SPARK_VERSION 0.8.1
ENV CDH_VERSION cdh4
ENV SCALA_HOME /opt/scala-$SCALA_VERSION
ENV SPARK_HOME /opt/spark
ENV PATH $SPARK_HOME:$SCALA_HOME/bin:$PATH

# Install Scala
ADD http://www.scala-lang.org/files/archive/scala-$SCALA_VERSION.tgz /
RUN (cd / && gunzip < scala-$SCALA_VERSION.tgz)|(cd /opt && tar -xvf -)
RUN rm /scala-$SCALA_VERSION.tgz
