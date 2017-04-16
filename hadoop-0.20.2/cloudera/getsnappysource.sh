#!/bin/sh
#
#  Licensed to Cloudera, Inc. under one or more contributor license
#  agreements.  See the NOTICE file distributed with this work for
#  additional information regarding copyright ownership.  Cloudera,
#  Inc. licenses this file to you under the Apache License, Version
#  2.0 (the "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

#
# Copyright (c) 2011 Cloudera, inc.
#

failIfNotOK() {
  if [ $? != 0 ]; then
    echo "Failed!"
    exit $?
  fi
}

SNAPPY_VERSION=$1
SNAPPY_JAVA_VERSION=$2
CACHEDIR=$3
BUILDDIR=$4

SNAPPY_SRC_TAR=snappy-${SNAPPY_VERSION}.tar.gz
SNAPPY_SRC_TAR_URL="http://snappy.googlecode.com/files/${SNAPPY_SRC_TAR}"
SNAPPY_JAVA_SRC_TAR=snappy-java-${SNAPPY_JAVA_VERSION}.tar.gz
SNAPPY_JAVA_SRC_TAR_URL="http://snappy-java.googlecode.com/files/${SNAPPY_JAVA_SRC_TAR}"

SNAPPY_SRC_TAR_PATH=${CACHEDIR}/${SNAPPY_SRC_TAR}
SNAPPY_JAVA_SRC_TAR_PATH=${CACHEDIR}/${SNAPPY_JAVA_SRC_TAR}

if [ ! -f ${SNAPPY_SRC_TAR_PATH} ]
then
  cd ${BUILDDIR}
  failIfNotOK
  wget ${SNAPPY_SRC_TAR_URL}
  failIfNotOK
  SNAPPY_SRC_TAR_PATH=${BUILDDIR}/${SNAPPY_SRC_TAR}
fi

if [ ! -f ${SNAPPY_JAVA_SRC_TAR_PATH} ]
then
  cd ${BUILDDIR}
  failIfNotOK
  wget -O ${SNAPPY_JAVA_SRC_TAR} ${SNAPPY_JAVA_SRC_TAR_URL} 
  failIfNotOK
  SNAPPY_JAVA_SRC_TAR_PATH=${BUILDDIR}/${SNAPPY_JAVA_SRC_TAR}
fi

cd ${BUILDDIR}
failIfNotOK

tar xzf ${SNAPPY_SRC_TAR_PATH}
failIfNotOK

tar xzf ${SNAPPY_JAVA_SRC_TAR_PATH}
failIfNotOK

exit 0
