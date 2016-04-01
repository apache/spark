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

import atexit
import os
import platform

import py4j

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.storagelevel import StorageLevel

if os.environ.get("SPARK_EXECUTOR_URI"):
    SparkContext.setSystemProperty("spark.executor.uri", os.environ["SPARK_EXECUTOR_URI"])

sc = SparkContext()
atexit.register(lambda: sc.stop())

try:
    # Try to access HiveConf, it will raise exception if Hive is not added
    sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
    sqlContext = HiveContext(sc)
except py4j.protocol.Py4JError:
    sqlContext = SQLContext(sc)
except TypeError:
    sqlContext = SQLContext(sc)

# for compatibility
sqlCtx = sqlContext

print("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version %s
      /_/
""" % sc.version)
print("Using Python version %s (%s, %s)" % (
    platform.python_version(),
    platform.python_build()[0],
    platform.python_build()[1]))
print("SparkContext available as sc, %s available as sqlContext." % sqlContext.__class__.__name__)

# The ./bin/pyspark script stores the old PYTHONSTARTUP value in OLD_PYTHONSTARTUP,
# which allows us to execute the user's PYTHONSTARTUP file:
_pythonstartup = os.environ.get('OLD_PYTHONSTARTUP')
if _pythonstartup and os.path.isfile(_pythonstartup):
    with open(_pythonstartup) as f:
        code = compile(f.read(), _pythonstartup, 'exec')
        exec(code)
