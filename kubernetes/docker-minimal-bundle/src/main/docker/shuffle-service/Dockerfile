FROM ubuntu:trusty

# Upgrade package index
# install a few other useful packages plus Open Jdk 7
# Remove unneeded /var/lib/apt/lists/* after install to reduce the
# docker image size (by ~30MB)
RUN apt-get update && \
    apt-get install -y less openjdk-7-jre-headless net-tools vim-tiny sudo openssh-server procps && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark
RUN mkdir -p /opt/spark/ui-resources/org/apache/spark/ui/static
RUN touch /opt/spark/RELEASE

ADD jars /opt/spark/jars
ADD bin /opt/spark/bin
ADD sbin /opt/spark/sbin
ADD conf /opt/spark/conf

ENV SPARK_HOME /opt/spark
ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64/jre

WORKDIR /opt/spark

CMD ${JAVA_HOME}/bin/java -Dspark.shuffle.service.port=$SPARK_SHUFFLE_SERVICE_PORT -Xms$SPARK_SHUFFLE_SERVICE_MEMORY -Xmx$SPARK_SHUFFLE_SERVICE_MEMORY -cp ${SPARK_HOME}/jars/\* org.apache.spark.deploy.ExternalShuffleService
