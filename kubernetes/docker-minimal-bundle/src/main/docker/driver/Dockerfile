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

CMD bin/spark-class org.apache.spark.deploy.kubernetes.driverlauncher.KubernetesDriverLauncherServer --driver-launcher-service-name $SPARK_DRIVER_LAUNCHER_SERVICE_NAME --application-secret-env $SPARK_DRIVER_LAUNCHER_APP_SECRET_ENV_NAME --driver-launcher-port $SPARK_DRIVER_LAUNCHER_SERVER_PORT
