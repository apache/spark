# This is an example Dockerfile for creating a Spark image which can be
# references by the Spark property 'spark.mesos.executor.docker.image'

FROM ubuntu

# Update the base ubuntu image with dependencies needed for Spark and Mesos
RUN apt-get update && \
    apt-get install -y python libnss3 openjdk-7-jre-headless

# A Spark distribution tarball is needed, one can be built from the root
# of the Spark source repository with `make-distribution.sh --tgz`
ADD spark-1.3.0.tar.gz /opt
RUN ln -s /opt/spark-1.3.0 /opt/spark
ENV SPARK_HOME /opt/spark

# The correct version of the Mesos shared library will be needed by this image
# One simple way to ensure you always have the correct version is to mount the
# system shared library into the image.
# In this example the shared library has been mounted to the volume:
# /host/usr/local/lib
ENV MESOS_NATIVE_JAVA_LIBRARY /host/usr/local/lib/libmesos.so
