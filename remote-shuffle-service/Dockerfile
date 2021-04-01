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
ARG java_image_tag=11-jre-slim

FROM openjdk:${java_image_tag}

ARG spark_uid=185

# Before building this docker image, first build this POM project to get the jar file
# using command like: mvn clean package -Premote-shuffle-service-server -DskipTests

RUN set -ex && \
    mkdir -p /opt/spark

WORKDIR /opt/spark/

COPY ./target/remote-shuffle-service-server.jar ./

EXPOSE 9338 9338
EXPOSE 8080 8080

# Specify the User that the actual main process will run as
USER ${spark_uid}

ENTRYPOINT ["java", "-Xmx8G", "-Dlog4j.configuration=log4j-remote-shuffle-service.properties", "-cp", "remote-shuffle-service-server.jar", "org.apache.spark.remoteshuffle.StreamServer", "-port", "9338", "-httpPort", "8080"]
