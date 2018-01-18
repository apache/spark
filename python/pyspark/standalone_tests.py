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
Standalone tests for PySpark - can be used to quickly test PySpark pip installation. When launched
without spark-submit verifies Jupyter redirection.
"""

from __future__ import print_function

import os
import sys


if sys.version >= "3":
    from io import StringIO
else:
    from StringIO import StringIO

if __name__ == "__main__":
    gateway_already_started = "PYSPARK_GATEWAY_PORT" in os.environ
    try:
        if not gateway_already_started and not hasattr(sys, "pypy_translation_info"):
            print("Running redirection tests since not in existing gateway")
            _old_stdout = sys.stdout
            _old_stderr = sys.stderr
            # Verify stdout/stderr overwrite support for jupyter
            sys.stdout = new_stdout = StringIO()
            sys.stderr = new_stderr = StringIO()
            print("Redirected to {0} / {1}".format(sys.stdout, sys.stderr), file=_old_stdout)
        elif hasattr(sys, "pypy_translation_info"):
            print("Skipping redirection tests in pypy")
        else:
            print("Skipping redirection tests since gateway already exists")

        from pyspark.sql import SparkSession
        if 'numpy' in sys.modules:
            from pyspark.ml.param import Params
            from pyspark.mllib.linalg import *
        else:
            print("Skipping pyspark ml import tests, missing numpy")

        spark = SparkSession\
            .builder\
            .appName("PipSanityCheck")\
            .getOrCreate()
        print("Spark context created")
        sc = spark.sparkContext
        rdd = sc.parallelize(range(100), 10)
        value = rdd.reduce(lambda x, y: x + y)

        if (value != 4950):
            print("Value {0} did not match expected value.".format(value), file=sys.__stderr__)
            sys.exit(-1)

        if not gateway_already_started:
            try:
                rdd2 = rdd.map(lambda x: str(x).startsWith("expected error"))
                rdd2.collect()
            except:
                pass

            sys.stdout = _old_stdout
            sys.stderr = _old_stderr
            logs = new_stderr.getvalue() + new_stdout.getvalue()

            if logs.find("'str' object has no attribute 'startsWith'") == -1 and \
               logs.find("SystemError: unknown opcode") == -1:
                print("Failed to find helpful error message, redirect failed?")
                print("logs were {0}".format(logs))
                sys.exit(-1)
            else:
                print("Redirection tests passed")
        print("Successfully ran pip sanity check")
    except Exception as inst:
        # If there is an uncaught exception print it, restore the stderr
        print("Exception during testing, {0}".format(inst), file=sys.__stderr__)
        sys.stderr = sys.__stderr__
        raise

    spark.stop()
