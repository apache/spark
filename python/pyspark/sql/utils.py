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

import py4j


class CapturedException(Exception):
    def __init__(self, desc, stackTrace):
        self.desc = desc
        self.stackTrace = stackTrace

    def __str__(self):
        return repr(self.desc)


class AnalysisException(CapturedException):
    """
    Failed to analyze a SQL query plan.
    """


class IllegalArgumentException(CapturedException):
    """
    Passed an illegal or inappropriate argument.
    """


def capture_sql_exception(f):
    def deco(*a, **kw):
        try:
            return f(*a, **kw)
        except py4j.protocol.Py4JJavaError as e:
            s = e.java_exception.toString()
            stackTrace = '\n\t at '.join(map(lambda x: x.toString(),
                                             e.java_exception.getStackTrace()))
            if s.startswith('org.apache.spark.sql.AnalysisException: '):
                raise AnalysisException(s.split(': ', 1)[1], stackTrace)
            if s.startswith('java.lang.IllegalArgumentException: '):
                raise IllegalArgumentException(s.split(': ', 1)[1], stackTrace)
            raise
    return deco


def install_exception_handler():
    """
    Hook an exception handler into Py4j, which could capture some SQL exceptions in Java.

    When calling Java API, it will call `get_return_value` to parse the returned object.
    If any exception happened in JVM, the result will be Java exception object, it raise
    py4j.protocol.Py4JJavaError. We replace the original `get_return_value` with one that
    could capture the Java exception and throw a Python one (with the same error message).

    It's idempotent, could be called multiple times.
    """
    original = py4j.protocol.get_return_value
    # The original `get_return_value` is not patched, it's idempotent.
    patched = capture_sql_exception(original)
    # only patch the one used in in py4j.java_gateway (call Java API)
    py4j.java_gateway.get_return_value = patched


def toJArray(gateway, jtype, arr):
    """
    Convert python list to java type array
    :param gateway: Py4j Gateway
    :param jtype: java type of element in array
    :param arr: python type list
    """
    jarr = gateway.new_array(jtype, len(arr))
    for i in range(0, len(arr)):
        jarr[i] = arr[i]
    return jarr
