# -*- coding: utf-8 -*-
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

import threading
import re
import sys
import traceback

from py4j.clientserver import ClientServer

__all__ = []


def print_exec(stream):
    ei = sys.exc_info()
    traceback.print_exception(ei[0], ei[1], ei[2], None, stream)


class VersionUtils(object):
    """
    Provides utility method to determine Spark versions with given input string.
    """
    @staticmethod
    def majorMinorVersion(sparkVersion):
        """
        Given a Spark version string, return the (major version number, minor version number).
        E.g., for 2.0.1-SNAPSHOT, return (2, 0).

        >>> sparkVersion = "2.4.0"
        >>> VersionUtils.majorMinorVersion(sparkVersion)
        (2, 4)
        >>> sparkVersion = "2.3.0-SNAPSHOT"
        >>> VersionUtils.majorMinorVersion(sparkVersion)
        (2, 3)

        """
        m = re.search(r'^(\d+)\.(\d+)(\..*)?$', sparkVersion)
        if m is not None:
            return (int(m.group(1)), int(m.group(2)))
        else:
            raise ValueError("Spark tried to parse '%s' as a Spark" % sparkVersion +
                             " version string, but it could not find the major and minor" +
                             " version numbers.")


def fail_on_stopiteration(f):
    """
    Wraps the input function to fail on 'StopIteration' by raising a 'RuntimeError'
    prevents silent loss of data when 'f' is used in a for loop in Spark code
    """
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except StopIteration as exc:
            raise RuntimeError(
                "Caught StopIteration thrown from user's code; failing the task",
                exc
            )

    return wrapper


def _print_missing_jar(lib_name, pkg_name, jar_name, spark_version):
    print("""
________________________________________________________________________________________________

  Spark %(lib_name)s libraries not found in class path. Try one of the following.

  1. Include the %(lib_name)s library and its dependencies with in the
     spark-submit command as

     $ bin/spark-submit --packages org.apache.spark:spark-%(pkg_name)s:%(spark_version)s ...

  2. Download the JAR of the artifact from Maven Central http://search.maven.org/,
     Group Id = org.apache.spark, Artifact Id = spark-%(jar_name)s, Version = %(spark_version)s.
     Then, include the jar in the spark-submit command as

     $ bin/spark-submit --jars <spark-%(jar_name)s.jar> ...

________________________________________________________________________________________________

""" % {
        "lib_name": lib_name,
        "pkg_name": pkg_name,
        "jar_name": jar_name,
        "spark_version": spark_version
    })


def _parse_memory(s):
    """
    Parse a memory string in the format supported by Java (e.g. 1g, 200m) and
    return the value in MiB

    >>> _parse_memory("256m")
    256
    >>> _parse_memory("2g")
    2048
    """
    units = {'g': 1024, 'm': 1, 't': 1 << 20, 'k': 1.0 / 1024}
    if s[-1].lower() not in units:
        raise ValueError("invalid format: " + s)
    return int(float(s[:-1]) * units[s[-1].lower()])


class InheritableThread(threading.Thread):
    """
    Thread that is recommended to be used in PySpark instead of :class:`threading.Thread`
    when the pinned thread mode is enabled. The usage of this class is exactly same as
    :class:`threading.Thread` but correctly inherits the inheritable properties specific
    to JVM thread such as ``InheritableThreadLocal``.

    Also, note that pinned thread mode does not close the connection from Python
    to JVM when the thread is finished in the Python side. With this class, Python
    garbage-collects the Python thread instance and also closes the connection
    which finishes JVM thread correctly.

    When the pinned thread mode is off, this works as :class:`threading.Thread`.

    .. note:: Experimental

    .. versionadded:: 3.1.0
    """
    def __init__(self, target, *args, **kwargs):
        from pyspark import SparkContext

        sc = SparkContext._active_spark_context

        if isinstance(sc._gateway, ClientServer):
            # Here's when the pinned-thread mode (PYSPARK_PIN_THREAD) is on.
            properties = sc._jsc.sc().getLocalProperties().clone()
            self._sc = sc

            def copy_local_properties(*a, **k):
                sc._jsc.sc().setLocalProperties(properties)
                return target(*a, **k)

            super(InheritableThread, self).__init__(
                target=copy_local_properties, *args, **kwargs)
        else:
            super(InheritableThread, self).__init__(target=target, *args, **kwargs)

    def __del__(self):
        from pyspark import SparkContext

        if isinstance(SparkContext._gateway, ClientServer):
            thread_connection = self._sc._jvm._gateway_client.thread_connection.connection()
            if thread_connection is not None:
                connections = self._sc._jvm._gateway_client.deque

                # Reuse the lock for Py4J in PySpark
                with SparkContext._lock:
                    for i in range(len(connections)):
                        if connections[i] is thread_connection:
                            connections[i].close()
                            del connections[i]
                            break
                    else:
                        # Just in case the connection was not closed but removed from the queue.
                        thread_connection.close()


if __name__ == "__main__":
    import doctest
    (failure_count, test_count) = doctest.testmod()
    if failure_count:
        sys.exit(-1)
