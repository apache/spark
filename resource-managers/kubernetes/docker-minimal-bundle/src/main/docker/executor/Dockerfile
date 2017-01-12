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

# TODO support spark.executor.extraClassPath
CMD exec ${JAVA_HOME}/bin/java -Dspark.executor.port=$SPARK_EXECUTOR_PORT -Xms$SPARK_EXECUTOR_MEMORY -Xmx$SPARK_EXECUTOR_MEMORY -cp ${SPARK_HOME}/jars/\* org.apache.spark.executor.CoarseGrainedExecutorBackend --driver-url $SPARK_DRIVER_URL --executor-id $SPARK_EXECUTOR_ID --cores $SPARK_EXECUTOR_CORES --app-id $SPARK_APPLICATION_ID --hostname $HOSTNAME
