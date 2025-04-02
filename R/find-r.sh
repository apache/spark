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

if [ -z "$R_SCRIPT_PATH" ]
then
  if [ ! -z "$R_HOME" ]
  then
    R_SCRIPT_PATH="$R_HOME/bin"
  else
    # if system wide R_HOME is not found, then exit
    if [ ! `command -v R` ]; then
      echo "Cannot find 'R_HOME'. Please specify 'R_HOME' or make sure R is properly installed."
      exit 1
    fi
    R_SCRIPT_PATH="$(dirname $(which R))"
  fi
  echo "Using R_SCRIPT_PATH = ${R_SCRIPT_PATH}"
fi
