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
import os

from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("PythonExecutableTest") \
        .getOrCreate()

    # Check python executable at executors
    is_custom_python_executor = spark.range(1).rdd.map(
        lambda _: "IS_CUSTOM_PYTHON" in os.environ).first()

    print("PYSPARK_PYTHON: %s" % os.environ.get("PYSPARK_PYTHON"))
    print("PYSPARK_DRIVER_PYTHON: %s" % os.environ.get("PYSPARK_DRIVER_PYTHON"))

    print("Custom Python used on executor: %s" % is_custom_python_executor)

    is_custom_python_driver = "IS_CUSTOM_PYTHON" in os.environ
    print("Custom Python used on driver: %s" % is_custom_python_driver)

    spark.stop()
