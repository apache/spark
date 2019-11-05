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

from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


if __name__ == "__main__":
    """
        Usage: pyfiles [major_python_version]
    """
    spark = SparkSession \
        .builder \
        .appName("PyFilesTest") \
        .getOrCreate()

    from py_container_checks import version_check
    # Begin of Python container checks
    version_check(sys.argv[1], 2 if sys.argv[1] == "python" else 3)

    # Check python executable at executors
    spark.udf.register("get_sys_ver",
                       lambda: "%d.%d" % sys.version_info[:2], StringType())
    [row] = spark.sql("SELECT get_sys_ver()").collect()
    driver_version = "%d.%d" % sys.version_info[:2]
    print("Python runtime version check for executor is: " + str(row[0] == driver_version))

    spark.stop()
