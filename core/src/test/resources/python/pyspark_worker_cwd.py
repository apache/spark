#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Minimal worker for ExecutorPythonWorkerSuite, launched through the stock PySpark daemon."""

import json
import os

from pyspark import SparkFiles
from pyspark.serializers import UTF8Deserializer, write_with_length


def read_artifact(path):
    try:
        with open(path, encoding="utf-8") as artifact:
            return artifact.read()
    except OSError as error:
        return f"{type(error).__name__}: {error}"


def main(infile, outfile):
    os.environ["SPARK_PYTHON_RUNTIME"] = "PYTHON_WORKER"
    from pyspark.worker_util import setup_spark_files

    setup_spark_files(infile)
    artifact = UTF8Deserializer().loads(infile)
    try:
        cwd = os.getcwd()
    except FileNotFoundError:
        cwd = "DELETED_CWD"
    result = {
        "cwd": cwd,
        "same_directory": os.path.samefile(".", SparkFiles.getRootDirectory()),
        "relative_artifact": read_artifact(artifact),
        "spark_files_artifact": read_artifact(SparkFiles.get(artifact)),
    }
    write_with_length(json.dumps(result).encode("utf-8"), outfile)
    outfile.flush()
