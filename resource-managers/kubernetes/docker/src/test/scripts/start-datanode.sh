#!/usr/bin/env bash
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
export JAVA_HOME=/usr/lib/jvm/jre-1.8.0-openjdk
export PATH=/hadoop/bin:$PATH
export HADOOP_CONF_DIR=/hadoop/etc/hadoop
mkdir -p /hadoop/etc/data
cp ${TMP_KRB_LOC} /etc/krb5.conf
cp ${TMP_CORE_LOC} /hadoop/etc/hadoop/core-site.xml
cp ${TMP_HDFS_LOC} /hadoop/etc/hadoop/hdfs-site.xml

until kinit -kt /var/keytabs/hdfs.keytab hdfs/nn.${NAMESPACE}.svc.cluster.local; do sleep 15; done

echo "KDC is up and ready to go... starting up"

kdestroy

hdfs datanode
