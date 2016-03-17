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

FROM mesosphere/mesos:0.20.1

# Update the base ubuntu image with dependencies needed for Spark
RUN apt-get update && \
    apt-get install -y python libnss3 openjdk-7-jre-headless curl

RUN mkdir /opt/spark && \
    curl http://www.apache.org/dyn/closer.lua/spark/spark-1.4.0/spark-1.4.0-bin-hadoop2.4.tgz \
    | tar -xzC /opt
ENV SPARK_HOME /opt/spark
ENV MESOS_NATIVE_JAVA_LIBRARY /usr/local/lib/libmesos.so
