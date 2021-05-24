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

FROM ubuntu:20.04

# Upgrade package index
# install a few other useful packages plus Open Java 11
# Remove unneeded /var/lib/apt/lists/* after install to reduce the
# docker image size (by ~30MB)
RUN apt-get update && \
    apt-get install -y less openjdk-11-jre-headless iproute2 vim-tiny sudo openssh-server && \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME /opt/spark
