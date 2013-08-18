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

"""
An interactive shell.

This file is designed to be launched as a PYTHONSTARTUP script.
"""
import os
import pyspark
from pyspark.context import SparkContext

# this is the equivalent of ADD_JARS
add_files = os.environ.get("ADD_FILES").split(',') if os.environ.get("ADD_FILES") != None else None

sc = SparkContext(os.environ.get("MASTER", "local"), "PySparkShell", pyFiles=add_files)
print "Spark context avaiable as sc."

if add_files != None:
    print "Adding files: [%s]" % ", ".join(add_files)

# The ./pyspark script stores the old PYTHONSTARTUP value in OLD_PYTHONSTARTUP,
# which allows us to execute the user's PYTHONSTARTUP file:
_pythonstartup = os.environ.get('OLD_PYTHONSTARTUP')
if _pythonstartup and os.path.isfile(_pythonstartup):
    execfile(_pythonstartup)
