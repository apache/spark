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

realpath () {
(
  TARGET_FILE="$1"

  cd "$(dirname "$TARGET_FILE")"
  TARGET_FILE="$(basename "$TARGET_FILE")"

  COUNT=0
  while [ -L "$TARGET_FILE" -a $COUNT -lt 100 ]
  do
      TARGET_FILE="$(readlink "$TARGET_FILE")"
      cd $(dirname "$TARGET_FILE")
      TARGET_FILE="$(basename $TARGET_FILE)"
      COUNT=$(($COUNT + 1))
  done

  echo "$(pwd -P)/"$TARGET_FILE""
)
}
