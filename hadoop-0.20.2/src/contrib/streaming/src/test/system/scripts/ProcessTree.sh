#!/bin/sh
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

##############################################################
## It creates the subprocess and keep increasing the memory ##
##############################################################
set -x
if [ $# -eq 0 ]
then
  StrVal="Hadoop is framework for data intensive distributed applications. \
Hadoop enables applications to work with thousands of nodes."
  i=1
else
  StrVal=$1
  i=$2
fi

if [ $i -lt 5 ]
then
   sh $0 "$StrVal-AppendingStr" `expr $i + 1`
else
   echo $StrVal
   while [ 1 ]
   do 
    sleep 5
   done
fi
