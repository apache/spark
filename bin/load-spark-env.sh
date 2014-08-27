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

# This script loads spark-env.sh if it exists, and ensures it is only loaded once.
# spark-env.sh is loaded from SPARK_CONF_DIR if set, or within the current directory's
# conf/ subdirectory.

if [ -z "$SPARK_ENV_LOADED" ]; then
  export SPARK_ENV_LOADED=1

  # Returns the parent of the directory this script lives in.
  parent_dir="$(cd `dirname $0`/..; pwd)"

  user_conf_dir=${SPARK_CONF_DIR:-"$parent_dir/conf"}

  if [ -f "${user_conf_dir}/spark-env.sh" ]; then
    # Promote all variable declarations to environment (exported) variables
    set -a
    . "${user_conf_dir}/spark-env.sh"
    set +a
  fi
fi
