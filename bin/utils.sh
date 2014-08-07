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
# http://docs.oracle.com/javase/7/docs/api/java/util/Properties.html#load(java.io.Reader).
# This accepts the name of the config and returns the value through JAVA_PROPERTY_VALUE.
# This currently does not support multi-line configs.
parse_java_property() {
  JAVA_PROPERTY_VALUE=$( \
    sed "/^[#!]/ d" "conf/spark-defaults.conf" | \
    grep "$1" | \
    sed "s/$1//" | \
    sed "s/^[[:space:]]*[:=]\{0,1\}//" | \
    sed "s/^[[:space:]]*\(.*\)[[:space:]]*$/\1/g" \
  )
  export JAVA_PROPERTY_VALUE
}

# Properly escape java options, dealing with whitespace, double quotes and backslashes.
# This accepts a string and returns the escaped list through ESCAPED_JAVA_OPTS.
escape_java_options() {
  ESCAPED_JAVA_OPTS=() # return value
  option_buffer=""     # buffer for collecting parts of an option
  opened_quotes=0      # whether we are expecting a closing double quotes
  for word in $1; do
    contains_quote=$(echo "$word" | grep \" | grep -v \\\\\")
    if [[ -n "$contains_quote" ]]; then
      # Flip the bit
      opened_quotes=$(((opened_quotes + 1) % 2))
    fi
    if [[ $opened_quotes == 0 ]]; then
      # Remove all non-escaped quotes around the value
      ESCAPED_JAVA_OPTS+=("$(echo "$option_buffer $word" | sed "s/^[[:space:]]*//" | sed "s/\([^\\]\)\"/\1/g")")
      option_buffer=""
    else
      # We are expecting a closing double quote, so keep buffering
      option_buffer="$option_buffer $word"
    fi
  done
  # Something is wrong if we ended with open double quotes
  if [[ $opened_quotes == 1 ]]; then
    echo "Java options parse error! Expecting closing double quotes." 1>&2
    exit 1
  fi
  export ESCAPED_JAVA_OPTS
}

# Put double quotes around each of the given java options that is a system property.
# This accepts a list and returns the quoted list through QUOTED_JAVA_OPTS
quote_java_property() {
  QUOTED_JAVA_OPTS=()
  for opt in "$@"; do
    is_system_property=$(echo "$opt" | grep -e "^-D")
    if [[ -n "$is_system_property" ]]; then
      QUOTED_JAVA_OPTS+=(\"$opt\")
    else
      QUOTED_JAVA_OPTS+=("$opt")
    fi
  done
  export QUOTED_JAVA_OPTS
}

