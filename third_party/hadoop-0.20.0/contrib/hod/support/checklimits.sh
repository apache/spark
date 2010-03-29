#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

COMMANDS=( "qstat" "qalter" "checkjob" )
ERROR=0
for (( i=0; i<${#COMMANDS[@]}; i++ ))
do
  cmd=${COMMANDS[$i]}
  CMD_PATH=`which $cmd 2>/dev/null`
  if [ $? -ne 0 ]
  then
    echo Could not find $cmd in PATH
    ERROR=1
  fi
done
if [ $ERROR -ne 0 ]
then
  exit 1
fi

jobs=`qstat -i |grep -o -e '^[0-9]*'`
for job in $jobs
do
  echo -en "$job\t"
  PATTERN="job [^ ]* violates active HARD MAXPROC limit of \([0-9]*\) for user [^ ]*[ ]*(R: \([0-9]*\), U: \([0-9]*\))"
  OUT=`checkjob $job 2>&1|grep -o -e "$PATTERN"`
  if [ $? -eq 0 ]
  then
    echo -en "| Exceeds resource limits\t"
    COMMENT_FIELD=`echo $OUT|sed -e "s/$PATTERN/User-limits exceeded. Requested:\2 Used:\3 MaxLimit:\1/"`
    qstat -f $job|grep '^[ \t]*comment = .*$' >/dev/null
    if [ $? -ne 0 ]
    then
      echo -en "| Comment field updated\t"
      qalter $job -W comment="$COMMENT_FIELD" >/dev/null
    else
      echo -en "| Comment field already set\t"
    fi
  else
    echo -en "| Doesn't exceed limits.\t"
  fi
  echo
done
