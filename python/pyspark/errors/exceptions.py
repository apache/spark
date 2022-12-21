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

from typing import cast, Optional

from pyspark import SparkContext

from py4j.java_gateway import JavaObject, is_instance_of


class PySparkException(Exception):
    """
    Base Exception for handling exceptions generated from PySpark.
    """

    def __init__(
        self,
        desc: Optional[str] = None,
        stackTrace: Optional[str] = None,
        origin: Optional[JavaObject] = None,
    ):
        # desc & stackTrace vs origin are mutually exclusive.
        assert (origin is not None and desc is None and stackTrace is None) or (
            origin is None and desc is not None and stackTrace is not None
        )

        self.desc = desc if desc is not None else cast(JavaObject, origin).getMessage()
        assert SparkContext._jvm is not None
        self.stackTrace = (
            stackTrace
            if stackTrace is not None
            else (SparkContext._jvm.org.apache.spark.util.Utils.exceptionString(origin))
        )
        self._origin = origin

    def getMessageParameters(self) -> Optional[dict]:
        assert SparkContext._gateway is not None

        gw = SparkContext._gateway
        if self._origin is not None and is_instance_of(
            gw, self._origin, "org.apache.spark.SparkThrowable"
        ):
            return self._origin.getMessageParameters()
        else:
            return None

    def __str__(self) -> str:
        assert SparkContext._jvm is not None

        jvm = SparkContext._jvm
        sql_conf = jvm.org.apache.spark.sql.internal.SQLConf.get()
        debug_enabled = sql_conf.pysparkJVMStacktraceEnabled()
        desc = self.desc
        if debug_enabled:
            desc = desc + "\n\nJVM stacktrace:\n%s" % self.stackTrace
        return str(desc)

    def getErrorClass(self) -> Optional[str]:
        assert SparkContext._gateway is not None

        gw = SparkContext._gateway
        if self._origin is not None and is_instance_of(
            gw, self._origin, "org.apache.spark.SparkThrowable"
        ):
            return self._origin.getErrorClass()
        else:
            return None

    def getSqlState(self) -> Optional[str]:
        assert SparkContext._gateway is not None

        gw = SparkContext._gateway
        if self._origin is not None and is_instance_of(
            gw, self._origin, "org.apache.spark.SparkThrowable"
        ):
            return self._origin.getSqlState()
        else:
            return None
