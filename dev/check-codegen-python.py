#!/usr/bin/env python3

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

# Utility for checking whether generated codes in PySpark are out of sync.
#   usage: ./dev/check-codegen-python.py

import os
import sys
import filecmp
import tempfile
import subprocess

# Location of your Spark git development area
SPARK_HOME = os.environ.get("SPARK_HOME", os.getcwd())


def fail(msg):
    print(msg)
    sys.exit(-1)


def run_cmd(cmd):
    print(f"RUN: {cmd}")
    if isinstance(cmd, list):
        return subprocess.check_output(cmd).decode("utf-8")
    else:
        return subprocess.check_output(cmd.split(" ")).decode("utf-8")


def check_connect_protos():
    print("Start checking the generated codes in pyspark-connect.")
    with tempfile.TemporaryDirectory() as tmp:
        run_cmd(f"{SPARK_HOME}/connector/connect/dev/generate_protos.sh {tmp}")
        result = filecmp.dircmp(
            f"{SPARK_HOME}/python/pyspark/sql/connect/proto/",
            tmp,
            ignore=["__init__.py", "__pycache__"],
        )
        success = True

        if len(result.left_only) > 0:
            print(f"Unexpected files: {result.left_only}")
            success = False

        if len(result.right_only) > 0:
            print(f"Missing files: {result.right_only}")
            success = False

        if len(result.funny_files) > 0:
            print(f"Incomparable files: {result.funny_files}")
            success = False

        if len(result.diff_files) > 0:
            print(f"Different files: {result.diff_files}")
            success = False

        if success:
            print("Finish checking the generated codes in pyspark-connect: SUCCESS")
        else:
            fail(
                "Generated files for pyspark-connect are out of sync! "
                "If you have touched files under connector/connect/src/main/protobuf, "
                "please run ./connector/connect/dev/generate_protos.sh. "
                "If you haven't touched any file above, please rebase your PR against main branch."
            )


check_connect_protos()

# TODO: also check generated code in pyspark-ml and pyspark-mllib.
