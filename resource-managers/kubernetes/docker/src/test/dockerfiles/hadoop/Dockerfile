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

FROM centos:7

ARG hadoop_version
ARG k_img_path=kubernetes/src/test

RUN yum -y install krb5-server krb5-workstation
RUN yum -y install java-1.8.0-openjdk-headless
RUN yum -y install apache-commons-daemon-jsvc
RUN yum install net-tools -y
RUN yum install telnet telnet-server -y
RUN yum -y install which

RUN sed -i -e 's/#//' -e 's/default_ccache_name/# default_ccache_name/' /etc/krb5.conf

RUN useradd -u 1098 hdfs

ADD hadoop-${hadoop_version}.tar.gz /
RUN ln -s hadoop-${hadoop_version} hadoop
RUN chown -R -L hdfs /hadoop

COPY ${k_img_path}/hadoop/conf/ssl-server.xml /hadoop/etc/hadoop/
COPY ${k_img_path}/hadoop/conf/yarn-site.xml /hadoop/etc/hadoop/

COPY ${k_img_path}/scripts/start-namenode.sh /
COPY ${k_img_path}/scripts/start-datanode.sh /
COPY ${k_img_path}/scripts/populate-data.sh /
COPY ${k_img_path}/scripts/start-kdc.sh /

COPY ${k_img_path}/data/people.txt /
