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

# Gather all all spark-submit options into SUBMISSION_OPTS
function gatherSparkSubmitOpts() {
  SUBMISSION_OPTS=()
  APPLICATION_OPTS=()
  while (($#)); do
    case $1 in
      --master | --deploy-mode | --class | --name | --jars | --py-files | --files)
        ;&

      --conf | --properties-file | --driver-memory | --driver-java-options)
        ;&

      --driver-library-path | --driver-class-path | --executor-memory | --driver-cores)
        ;&

      --total-executor-cores | --executor-cores | --queue | --num-executors | --archives)
        if [[ $# -lt 2 ]]; then
          usage
          exit 1;
        fi
        SUBMISSION_OPTS+=($1); shift
        SUBMISSION_OPTS+=($1); shift
        ;;

      --verbose | -v | --supervise)
        SUBMISSION_OPTS+=($1); shift
        ;;

      *)
        APPLICATION_OPTS+=($1); shift
        ;;
    esac
  done

  export SUBMISSION_OPTS
  export APPLICATION_OPTS
}
