# This is an example Dockerfile for creating a Spark image which can be
# references by the Spark property 'spark.mesos.executor.docker.image'
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
