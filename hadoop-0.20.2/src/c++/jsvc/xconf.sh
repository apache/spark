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
# This script tries to configure the C/C++ part of the Apache commons-daemon
# for a cross compilation of a 32bit target on a 64bit platform

export LDFLAGS="-m${JSVC_ARCH}"

if [ "$JSVC_ARCH" = "32" ] ; then
  HOST=`sh support/config.guess | sed -e 's#^[^-]*-#i386-#'` 
  CONFIGURE_OPTS="--host=$HOST"
fi
sh ./configure ${CONFIGURE_OPTS}
