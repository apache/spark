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

echo "Start to download sbt dependencies corrupted file."

build/mvn dependency:get -DremoteRepositories=https://maven-central.storage-download.googleapis.com/maven2/ -DgroupId=com.lihaoyi -DartifactId=geny_2.13 -Dversion=0.7.1 -Dtransitive=false --quiet

build/mvn dependency:get -DremoteRepositories=https://maven-central.storage-download.googleapis.com/maven2/ -DgroupId=io.netty -DartifactId=netty-buffer -Dversion=4.1.96.Final -Dtransitive=false --quiet
build/mvn dependency:get -DremoteRepositories=https://maven-central.storage-download.googleapis.com/maven2/ -DgroupId=io.netty -DartifactId=netty-codec -Dversion=4.1.96.Final -Dtransitive=false --quiet
build/mvn dependency:get -DremoteRepositories=https://maven-central.storage-download.googleapis.com/maven2/ -DgroupId=io.netty -DartifactId=netty-codec-http -Dversion=4.1.96.Final -Dtransitive=false --quiet
build/mvn dependency:get -DremoteRepositories=https://maven-central.storage-download.googleapis.com/maven2/ -DgroupId=io.netty -DartifactId=netty-codec-socks -Dversion=4.1.60.Final -Dtransitive=false --quiet
build/mvn dependency:get -DremoteRepositories=https://maven-central.storage-download.googleapis.com/maven2/ -DgroupId=io.netty -DartifactId=netty-codec-socks -Dversion=4.1.96.Final -Dtransitive=false --quiet
build/mvn dependency:get -DremoteRepositories=https://maven-central.storage-download.googleapis.com/maven2/ -DgroupId=io.netty -DartifactId=netty-handler -Dversion=4.1.96.Final -Dtransitive=false --quiet
build/mvn dependency:get -DremoteRepositories=https://maven-central.storage-download.googleapis.com/maven2/ -DgroupId=io.netty -DartifactId=netty-handler-proxy -Dversion=4.1.60.Final -Dtransitive=false --quiet
build/mvn dependency:get -DremoteRepositories=https://maven-central.storage-download.googleapis.com/maven2/ -DgroupId=io.netty -DartifactId=netty-resolver -Dversion=4.1.96.Final -Dtransitive=false --quiet
build/mvn dependency:get -DremoteRepositories=https://maven-central.storage-download.googleapis.com/maven2/ -DgroupId=io.netty -DartifactId=netty-transport -Dversion=4.1.96.Final -Dtransitive=false --quiet
build/mvn dependency:get -DremoteRepositories=https://maven-central.storage-download.googleapis.com/maven2/ -DgroupId=io.netty -DartifactId=netty-transport-classes-epoll -Dversion=4.1.96.Final -Dtransitive=false --quiet
build/mvn dependency:get -DremoteRepositories=https://maven-central.storage-download.googleapis.com/maven2/ -DgroupId=io.netty -DartifactId=netty-transport-classes-kqueue -Dversion=4.1.96.Final -Dtransitive=false --quiet
build/mvn dependency:get -DremoteRepositories=https://maven-central.storage-download.googleapis.com/maven2/ -DgroupId=io.netty -DartifactId=netty-transport-native-epoll -Dversion=4.1.96.Final -Dtransitive=false --quiet
build/mvn dependency:get -DremoteRepositories=https://maven-central.storage-download.googleapis.com/maven2/ -DgroupId=io.netty -DartifactId=netty-transport-native-epoll -Dversion=4.1.99.Final -Dtransitive=false --quiet
build/mvn dependency:get -DremoteRepositories=https://maven-central.storage-download.googleapis.com/maven2/ -DgroupId=io.netty -DartifactId=netty-transport-native-kqueue -Dversion=4.1.96.Final -Dtransitive=false --quiet
build/mvn dependency:get -DremoteRepositories=https://maven-central.storage-download.googleapis.com/maven2/ -DgroupId=io.netty -DartifactId=netty-transport-native-kqueue -Dversion=4.1.99.Final -Dtransitive=false --quiet
build/mvn dependency:get -DremoteRepositories=https://maven-central.storage-download.googleapis.com/maven2/ -DgroupId=io.netty -DartifactId=netty-transport-native-unix-common -Dversion=4.1.96.Final -Dtransitive=false --quiet

build/mvn dependency:get -DremoteRepositories=https://maven-central.storage-download.googleapis.com/maven2/ -DgroupId=org.scala-lang.modules -DartifactId=scala-collection-compat_2.13 -Dversion=2.9.0 -Dtransitive=false --quiet

echo "Finish to download sbt dependencies corrupted file."
