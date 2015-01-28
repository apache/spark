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

# Gather all spark-submit options into SUBMISSION_OPTS
function gatherSparkSubmitOpts() {

  if [ -z "$SUBMIT_USAGE_FUNCTION" ]; then
    echo "Function for printing usage of $0 is not set." 1>&2
    echo "Please set usage function to shell variable 'SUBMIT_USAGE_FUNCTION' in $0" 1>&2
    exit 1
  fi

  # NOTE: If you add or remove spark-submit options,
  # modify NOT ONLY this script but also SparkSubmitArgument.scala
  SUBMISSION_OPTS=()
  APPLICATION_OPTS=()
  while (($#)); do
    case "$1" in
      --master | --deploy-mode | --class | --name | --jars | --packages | --py-files | --files | \
      --conf | --repositories | --properties-file | --driver-memory | --driver-java-options | \
      --driver-library-path | --driver-class-path | --executor-memory | --driver-cores | \
      --total-executor-cores | --executor-cores | --queue | --num-executors | --archives)
        if [[ $# -lt 2 ]]; then
          "$SUBMIT_USAGE_FUNCTION"
          exit 1;
        fi
        SUBMISSION_OPTS+=("$1"); shift
        SUBMISSION_OPTS+=("$1"); shift
        ;;

      --verbose | -v | --supervise)
        SUBMISSION_OPTS+=("$1"); shift
        ;;

      *)
        APPLICATION_OPTS+=("$1"); shift
        ;;
    esac
  done

  export SUBMISSION_OPTS
  export APPLICATION_OPTS
}
