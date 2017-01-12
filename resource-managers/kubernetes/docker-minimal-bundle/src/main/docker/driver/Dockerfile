FROM anapsix/alpine-java:8

RUN mkdir -p /opt/spark
RUN mkdir -p /opt/spark/ui-resources/org/apache/spark/ui/static
RUN touch /opt/spark/RELEASE

ADD jars /opt/spark/jars
ADD bin /opt/spark/bin
ADD sbin /opt/spark/sbin
ADD conf /opt/spark/conf

ENV SPARK_HOME /opt/spark

WORKDIR /opt/spark

# This class will also require setting a secret via the SPARK_APP_SECRET environment variable
CMD exec bin/spark-class org.apache.spark.deploy.rest.kubernetes.KubernetesSparkRestServer --hostname $HOSTNAME --port $SPARK_DRIVER_LAUNCHER_SERVER_PORT --secret-file $SPARK_SUBMISSION_SECRET_LOCATION
