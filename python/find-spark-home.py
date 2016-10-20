#!/usr/bin/python

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

# This script attempt to determine the correct setting for SPARK_HOME given
# that Spark may have been installed on the system with pip.

from __future__ import print_function
import os, sys

def is_spark_home(path):
    """Takes a path and returns true if the provided path could be a reasonable SPARK_HOME"""
    return (os.path.isfile(path + "/bin/spark-submit") and os.path.isdir(path + "/jars"))

paths = ["../", os.path.dirname(sys.argv[0]) + "/../"]

# Add the path of the PySpark module if it exists
import sys
if sys.version < "3":
    import imp
    try:
        paths.append(imp.find_module("pyspark")[1])
    except ImportError:
        # Not pip installed no worries
        True
else:
    import importlib
    try:
        paths.append(importlib.util.find_spec("pyspark").origin)
    except ImportError:
        # Not pip installed no worries
        True

# Normalize the paths
paths = map(lambda path:os.path.abspath(path), paths)

try:
    print(next(path for path in paths if is_spark_home(path)))
except StopIteration:
    print("Could not find valid SPARK_HOME while searching %s" % paths, file=sys.stderr)
    exit(-1)
