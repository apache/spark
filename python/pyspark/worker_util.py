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
Util functions for workers.
"""
import sys
from typing import IO

from pyspark.errors import PySparkRuntimeError
from pyspark.serializers import (
    UTF8Deserializer,
    CPickleSerializer,
)

pickleSer = CPickleSerializer()
utf8_deserializer = UTF8Deserializer()


def check_python_version(infile: IO) -> None:
    """
    Check the Python version between the running process and the one used to serialize the command.
    """
    version = utf8_deserializer.loads(infile)
    if version != "%d.%d" % sys.version_info[:2]:
        raise PySparkRuntimeError(
            error_class="PYTHON_VERSION_MISMATCH",
            message_parameters={
                "worker_version": str(sys.version_info[:2]),
                "driver_version": str(version),
            },
        )
