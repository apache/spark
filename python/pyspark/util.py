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

import functools
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


def inheritable_thread_target(f):
    """
    Return thread target wrapper which is recommended to be used in PySpark when the
    pinned thread mode is enabled. The wrapper function, before calling original
    thread target, it inherits the inheritable properties specific
    to JVM thread such as ``InheritableThreadLocal``.

    Also, note that pinned thread mode does not close the connection from Python
    to JVM when the thread is finished in the Python side. With this wrapper, Python
    garbage-collects the Python thread instance and also closes the connection
    which finishes JVM thread correctly.

    When the pinned thread mode is off, it return the original ``f``.

    .. versionadded:: 3.2.0

    Parameters
    ----------
    f : function
        the original thread target.

    Notes
    -----
    This API is experimental.

    It is important to know that it captures the local properties when you decorate it
    whereas :class:`InheritableThread` captures when the thread is started.
    Therefore, it is encouraged to decorate it when you want to capture the local
    properties.

    For example, the local properties from the current Spark context is captured
    when you define a function here instead of the invocation:

    >>> @inheritable_thread_target
    ... def target_func():
    ...     pass  # your codes.

    If you have any updates on local properties afterwards, it would not be reflected to
    the Spark context in ``target_func()``.

    The example below mimics the behavior of JVM threads as close as possible:

    >>> Thread(target=inheritable_thread_target(target_func)).start()  # doctest: +SKIP
    """
    from pyspark import SparkContext

    if isinstance(SparkContext._gateway, ClientServer):
        # Here's when the pinned-thread mode (PYSPARK_PIN_THREAD) is on.

        # NOTICE the internal difference vs `InheritableThread`. `InheritableThread`
        # copies local properties when the thread starts but `inheritable_thread_target`
        # copies when the function is wrapped.
        properties = SparkContext._active_spark_context._jsc.sc().getLocalProperties().clone()

        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            try:
                # Set local properties in child thread.
                SparkContext._active_spark_context._jsc.sc().setLocalProperties(properties)
                return f(*args, **kwargs)
            finally:
                InheritableThread._clean_py4j_conn_for_current_thread()
        return wrapped
    else:
        return f


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

        if isinstance(SparkContext._gateway, ClientServer):
            # Here's when the pinned-thread mode (PYSPARK_PIN_THREAD) is on.
            def copy_local_properties(*a, **k):
                # self._props is set before starting the thread to match the behavior with JVM.
                assert hasattr(self, "_props")
                SparkContext._active_spark_context._jsc.sc().setLocalProperties(self._props)
                try:
                    return target(*a, **k)
                finally:
                    InheritableThread._clean_py4j_conn_for_current_thread()

            super(InheritableThread, self).__init__(
                target=copy_local_properties, *args, **kwargs)
        else:
            super(InheritableThread, self).__init__(target=target, *args, **kwargs)

    def start(self, *args, **kwargs):
        from pyspark import SparkContext

        if isinstance(SparkContext._gateway, ClientServer):
            # Here's when the pinned-thread mode (PYSPARK_PIN_THREAD) is on.

            # Local property copy should happen in Thread.start to mimic JVM's behavior.
            self._props = SparkContext._active_spark_context._jsc.sc().getLocalProperties().clone()
        return super(InheritableThread, self).start(*args, **kwargs)

    @staticmethod
    def _clean_py4j_conn_for_current_thread():
        from pyspark import SparkContext

        jvm = SparkContext._jvm
        thread_connection = jvm._gateway_client.get_thread_connection()
        if thread_connection is not None:
            try:
                # Dequeue is shared across other threads but it's thread-safe.
                # If this function has to be invoked one more time in the same thead
                # Py4J will create a new connection automatically.
                jvm._gateway_client.deque.remove(thread_connection)
            except ValueError:
                # Should never reach this point
                return
            finally:
                thread_connection.close()


if __name__ == "__main__":
    if "pypy" not in platform.python_implementation().lower() and sys.version_info[:2] >= (3, 7):
        import doctest
        import pyspark.util
        from pyspark.context import SparkContext

        globs = pyspark.util.__dict__.copy()
        globs['sc'] = SparkContext('local[4]', 'PythonTest')
        (failure_count, test_count) = doctest.testmod(pyspark.util, globs=globs)
        globs['sc'].stop()

        if failure_count:
            sys.exit(-1)
