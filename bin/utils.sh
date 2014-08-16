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

# * ---------------------------------------------------- *
# |  Utility functions for launching Spark applications  |
# * ---------------------------------------------------- *

# Parse the value of a config from a java properties file according to the specifications in
# http://docs.oracle.com/javase/7/docs/api/java/util/Properties.html#load(java.io.Reader),
# with the exception of the support for multi-line arguments. This accepts the name of the
# config as an argument, and expects the path of the property file to be found in
# PROPERTIES_FILE. The value is returned through JAVA_PROPERTY_VALUE.
function parse_java_property() {
  JAVA_PROPERTY_VALUE=$(\
    grep "^[[:space:]]*$1" "$PROPERTIES_FILE" | \
    head -n 1 | \
    sed "s/^[[:space:]]*$1//g" | \
    sed "s/^[[:space:]]*[:=]\{0,1\}//g" | \
    sed "s/^[[:space:]]*//g" | \
    sed "s/[[:space:]]*$//g"
  )
  export JAVA_PROPERTY_VALUE
}

# Properly split java options, dealing with whitespace, double quotes and backslashes.
# This accepts a string and returns the resulting list through SPLIT_JAVA_OPTS.
# For security reasons, this is isolated in its own function.
function split_java_options() {
  eval set -- "$1"
  SPLIT_JAVA_OPTS=("$@")
  export SPLIT_JAVA_OPTS
}

# Put double quotes around each of the given java options that is a system property.
# This accepts a list and returns the quoted list through QUOTED_JAVA_OPTS
function quote_java_property() {
  QUOTED_JAVA_OPTS=()
  for opt in "$@"; do
    is_system_property=$(echo "$opt" | grep -e "^-D")
    if [[ -n "$is_system_property" ]]; then
      QUOTED_JAVA_OPTS+=("\"$opt\"")
    else
      QUOTED_JAVA_OPTS+=("$opt")
    fi
  done
  export QUOTED_JAVA_OPTS
}

# Gather all all spark-submit options into SUBMISSION_OPTS
function gatherSparkSubmitOpts() {
  if [ -z "$SUBMIT_USAGE_FUNCTION" ]; then
    echo "Function for printing usage of $0 is not set." 1>&2
    echo "Please set usage function to shell variable 'SUBMIT_USAGE_FUNCTION' in $0" 1>&2
    exit 1
  fi

  # NOTE: If you add or remove spark-sumbmit options,
  # modify NOT ONLY this script but also SparkSubmitArgument.scala
  SUBMISSION_OPTS=()
  APPLICATION_OPTS=()
  while (($#)); do
    case "$1" in
      --master | --deploy-mode | --class | --name | --jars | --py-files | --files | \
      --conf | --properties-file | --driver-memory | --driver-java-options | \
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

