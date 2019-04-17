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

#
# This is an example script that can be used to discover GPUs. It only works on NVIDIA GPUs since it
# uses the nvidia-smi command. This script will find all visible GPUs so if you aren't running
# in an environment that can isolate GPUs to an executor and where multiple executors can run on a
# single node you may not want to use this. See your cluster manager specific configs for other
# options.
#
# It can be passed into Spark via the configs spark.executor.resource.gpu.discoveryScript and/or
# spark.driver.resource.gpu.discoveryScript.
# The script will return the format count:unit:comma-separated string of the GPU indices available where it was executed.
# If used with the executors, Spark will assigned out the indices to tasks based on the config to
# control the number of GPUs per task. The driver
#
#
ADDRS=`nvidia-smi --query-gpu=index --format=csv,noheader | sed 'N;s/\n/,/'`
COUNT=`echo $ADDRS | tr -cd , | wc -c`
ALLCOUNT=`expr $COUNT + 1`
echo $ALLCOUNT::$ADDRS