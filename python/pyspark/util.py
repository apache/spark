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

import itertools
import os
import platform
import re
import sys
import threading
import traceback
import types

from py4j.clientserver import ClientServer

__all__ = []  # type: ignore


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

        Examples
        --------
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


def walk_tb(tb):
    while tb is not None:
        yield tb
        tb = tb.tb_next


def try_simplify_traceback(tb):
    """
    Simplify the traceback. It removes the tracebacks in the current package, and only
    shows the traceback that is related to the thirdparty and user-specified codes.

    Returns
    -------
    TracebackType or None
      Simplified traceback instance. It returns None if it fails to simplify.

    Notes
    -----
    This keeps the tracebacks once it sees they are from a different file even
    though the following tracebacks are from the current package.

    Examples
    --------
    >>> import importlib
    >>> import sys
    >>> import traceback
    >>> import tempfile
    >>> with tempfile.TemporaryDirectory() as tmp_dir:
    ...     with open("%s/dummy_module.py" % tmp_dir, "w") as f:
    ...         _ = f.write(
    ...             'def raise_stop_iteration():\\n'
    ...             '    raise StopIteration()\\n\\n'
    ...             'def simple_wrapper(f):\\n'
    ...             '    def wrapper(*a, **k):\\n'
    ...             '        return f(*a, **k)\\n'
    ...             '    return wrapper\\n')
    ...         f.flush()
    ...         spec = importlib.util.spec_from_file_location(
    ...             "dummy_module", "%s/dummy_module.py" % tmp_dir)
    ...         dummy_module = importlib.util.module_from_spec(spec)
    ...         spec.loader.exec_module(dummy_module)
    >>> def skip_doctest_traceback(tb):
    ...     import pyspark
    ...     root = os.path.dirname(pyspark.__file__)
    ...     pairs = zip(walk_tb(tb), traceback.extract_tb(tb))
    ...     for cur_tb, cur_frame in pairs:
    ...         if cur_frame.filename.startswith(root):
    ...             return cur_tb

    Regular exceptions should show the file name of the current package as below.

    >>> exc_info = None
    >>> try:
    ...     fail_on_stopiteration(dummy_module.raise_stop_iteration)()
    ... except Exception as e:
    ...     tb = sys.exc_info()[-1]
    ...     e.__cause__ = None
    ...     exc_info = "".join(
    ...         traceback.format_exception(type(e), e, tb))
    >>> print(exc_info)  # doctest: +NORMALIZE_WHITESPACE, +ELLIPSIS
    Traceback (most recent call last):
      File ...
        ...
      File "/.../pyspark/util.py", line ...
        ...
    RuntimeError: ...
    >>> "pyspark/util.py" in exc_info
    True

    If the traceback is simplified with this method, it hides the current package file name:

    >>> exc_info = None
    >>> try:
    ...     fail_on_stopiteration(dummy_module.raise_stop_iteration)()
    ... except Exception as e:
    ...     tb = try_simplify_traceback(sys.exc_info()[-1])
    ...     e.__cause__ = None
    ...     exc_info = "".join(
    ...         traceback.format_exception(
    ...             type(e), e, try_simplify_traceback(skip_doctest_traceback(tb))))
    >>> print(exc_info)  # doctest: +NORMALIZE_WHITESPACE, +ELLIPSIS
    RuntimeError: ...
    >>> "pyspark/util.py" in exc_info
    False

    In the case below, the traceback contains the current package in the middle.
    In this case, it just hides the top occurrence only.

    >>> exc_info = None
    >>> try:
    ...     fail_on_stopiteration(dummy_module.simple_wrapper(
    ...         fail_on_stopiteration(dummy_module.raise_stop_iteration)))()
    ... except Exception as e:
    ...     tb = sys.exc_info()[-1]
    ...     e.__cause__ = None
    ...     exc_info_a = "".join(
    ...         traceback.format_exception(type(e), e, tb))
    ...     exc_info_b = "".join(
    ...         traceback.format_exception(
    ...             type(e), e, try_simplify_traceback(skip_doctest_traceback(tb))))
    >>> exc_info_a.count("pyspark/util.py")
    2
    >>> exc_info_b.count("pyspark/util.py")
    1
    """
    if "pypy" in platform.python_implementation().lower():
        # Traceback modification is not supported with PyPy in PySpark.
        return None
    if sys.version_info[:2] < (3, 7):
        # Traceback creation is not supported Python < 3.7.
        # See https://bugs.python.org/issue30579.
        return None

    import pyspark

    root = os.path.dirname(pyspark.__file__)
    tb_next = None
    new_tb = None
    pairs = zip(walk_tb(tb), traceback.extract_tb(tb))
    last_seen = []

    for cur_tb, cur_frame in pairs:
        if not cur_frame.filename.startswith(root):
            # Filter the stacktrace from the PySpark source itself.
            last_seen = [(cur_tb, cur_frame)]
            break

    for cur_tb, cur_frame in reversed(list(itertools.chain(last_seen, pairs))):
        # Once we have seen the file names outside, don't skip.
        new_tb = types.TracebackType(
            tb_next=tb_next,
            tb_frame=cur_tb.tb_frame,
            tb_lasti=cur_tb.tb_frame.f_lasti,
            tb_lineno=cur_tb.tb_frame.f_lineno)
        tb_next = new_tb
    return new_tb


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

    Examples
    --------
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

    .. versionadded:: 3.1.0


    Notes
    -----
    This API is experimental.
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

    if "pypy" not in platform.python_implementation().lower() and sys.version_info[:2] >= (3, 7):
        (failure_count, test_count) = doctest.testmod()
        if failure_count:
            sys.exit(-1)
