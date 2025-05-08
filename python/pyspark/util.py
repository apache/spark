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

import copy
import functools
import itertools
import os
import platform
import re
import sys
import threading
import traceback
import typing
import socket
import warnings
from types import TracebackType
from typing import Any, Callable, IO, Iterator, List, Optional, TextIO, Tuple, Union

from pyspark.errors import PySparkRuntimeError
from pyspark.serializers import (
    write_int,
    read_int,
    write_with_length,
    SpecialLengths,
    UTF8Deserializer,
)

__all__: List[str] = []

if typing.TYPE_CHECKING:
    import io

    from py4j.java_collections import JavaArray
    from py4j.java_gateway import JavaObject

    from pyspark._typing import NonUDFType
    from pyspark.sql.pandas._typing import (
        PandasScalarUDFType,
        PandasGroupedMapUDFType,
        PandasGroupedAggUDFType,
        PandasWindowAggUDFType,
        PandasScalarIterUDFType,
        PandasMapIterUDFType,
        PandasCogroupedMapUDFType,
        ArrowMapIterUDFType,
        PandasGroupedMapUDFWithStateType,
        ArrowGroupedMapUDFType,
        ArrowCogroupedMapUDFType,
        PandasGroupedMapUDFTransformWithStateType,
        PandasGroupedMapUDFTransformWithStateInitStateType,
        GroupedMapUDFTransformWithStateType,
        GroupedMapUDFTransformWithStateInitStateType,
    )
    from pyspark.sql._typing import (
        SQLArrowBatchedUDFType,
        SQLArrowTableUDFType,
        SQLBatchedUDFType,
        SQLTableUDFType,
    )
    from pyspark.serializers import Serializer
    from pyspark.sql import SparkSession


JVM_BYTE_MIN: int = -(1 << 7)
JVM_BYTE_MAX: int = (1 << 7) - 1
JVM_SHORT_MIN: int = -(1 << 15)
JVM_SHORT_MAX: int = (1 << 15) - 1
JVM_INT_MIN: int = -(1 << 31)
JVM_INT_MAX: int = (1 << 31) - 1
JVM_LONG_MIN: int = -(1 << 63)
JVM_LONG_MAX: int = (1 << 63) - 1


def print_exec(stream: TextIO) -> None:
    ei = sys.exc_info()
    traceback.print_exception(ei[0], ei[1], ei[2], None, stream)


class VersionUtils:
    """
    Provides utility method to determine Spark versions with given input string.
    """

    @staticmethod
    def majorMinorVersion(sparkVersion: str) -> Tuple[int, int]:
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
        m = re.search(r"^(\d+)\.(\d+)(\..*)?$", sparkVersion)
        if m is not None:
            return (int(m.group(1)), int(m.group(2)))
        else:
            raise ValueError(
                "Spark tried to parse '%s' as a Spark" % sparkVersion
                + " version string, but it could not find the major and minor"
                + " version numbers."
            )


def fail_on_stopiteration(f: Callable) -> Callable:
    """
    Wraps the input function to fail on 'StopIteration' by raising a 'RuntimeError'
    prevents silent loss of data when 'f' is used in a for loop in Spark code
    """

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return f(*args, **kwargs)
        except StopIteration as exc:
            raise PySparkRuntimeError(
                errorClass="STOP_ITERATION_OCCURRED",
                messageParameters={
                    "exc": str(exc),
                },
            )

    return wrapper


def walk_tb(tb: Optional[TracebackType]) -> Iterator[TracebackType]:
    while tb is not None:
        yield tb
        tb = tb.tb_next


def try_simplify_traceback(tb: TracebackType) -> Optional[TracebackType]:
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
    >>> with tempfile.TemporaryDirectory(prefix="try_simplify_traceback") as tmp_dir:
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
    pyspark.errors.exceptions.base.PySparkRuntimeError: ...
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
    pyspark.errors.exceptions.base.PySparkRuntimeError: ...
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
        new_tb = TracebackType(
            tb_next=tb_next,
            tb_frame=cur_tb.tb_frame,
            tb_lasti=cur_tb.tb_frame.f_lasti,
            tb_lineno=cur_tb.tb_frame.f_lineno if cur_tb.tb_frame.f_lineno is not None else -1,
        )
        tb_next = new_tb
    return new_tb


def _print_missing_jar(lib_name: str, pkg_name: str, jar_name: str, spark_version: str) -> None:
    print(
        """
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

"""
        % {
            "lib_name": lib_name,
            "pkg_name": pkg_name,
            "jar_name": jar_name,
            "spark_version": spark_version,
        }
    )


def _parse_memory(s: str) -> int:
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
    units = {"g": 1024, "m": 1, "t": 1 << 20, "k": 1.0 / 1024}
    if s[-1].lower() not in units:
        raise ValueError("invalid format: " + s)
    return int(float(s[:-1]) * units[s[-1].lower()])


def inheritable_thread_target(f: Optional[Union[Callable, "SparkSession"]] = None) -> Callable:
    """
    Return thread target wrapper which is recommended to be used in PySpark when the
    pinned thread mode is enabled. The wrapper function, before calling original
    thread target, it inherits the inheritable properties specific
    to JVM thread such as ``InheritableThreadLocal``, or thread local such as tags
    with Spark Connect.

    When the pinned thread mode is off, it return the original ``f``.

    .. versionadded:: 3.2.0

    .. versionchanged:: 3.5.0
        Supports Spark Connect.

    Parameters
    ----------
    f : function, or :class:`SparkSession`
        the original thread target, or :class:`SparkSession` if Spark Connect is being used.
        See the examples below.

    Notes
    -----
    This API is experimental.

    It is important to know that it captures the local properties or tags when you
    decorate it whereas :class:`InheritableThread` captures when the thread is started.
    Therefore, it is encouraged to decorate it when you want to capture the local
    properties.

    For example, the local properties or tags from the current Spark context or Spark
    session is captured when you define a function here instead of the invocation:

    >>> @inheritable_thread_target
    ... def target_func():
    ...     pass  # your codes.

    If you have any updates on local properties or tags afterwards, it would not be
    reflected to the Spark context in ``target_func()``.

    The example below mimics the behavior of JVM threads as close as possible:

    >>> Thread(target=inheritable_thread_target(target_func)).start()  # doctest: +SKIP

    If you're using Spark Connect or if you want to inherit the tags properly,
    you should explicitly provide Spark session as follows:

    >>> @inheritable_thread_target(session)  # doctest: +SKIP
    ... def target_func():
    ...     pass  # your codes.

    >>> Thread(target=inheritable_thread_target(session)(target_func)).start()  # doctest: +SKIP
    """
    from pyspark.sql import is_remote

    # Spark Connect
    if is_remote():
        session = f
        assert session is not None, "Spark Connect session must be provided."

        def outer(ff: Callable) -> Callable:
            thread_local = session.client.thread_local  # type: ignore[union-attr, operator]
            session_client_thread_local_attrs = [
                (attr, copy.deepcopy(value))
                for (
                    attr,
                    value,
                ) in thread_local.__dict__.items()
            ]

            @functools.wraps(ff)
            def inner(*args: Any, **kwargs: Any) -> Any:
                # Propagates the active remote spark session to the current thread.
                from pyspark.sql.connect.session import SparkSession as RemoteSparkSession

                RemoteSparkSession._set_default_and_active_session(
                    session  # type: ignore[arg-type]
                )
                # Set thread locals in child thread.
                for attr, value in session_client_thread_local_attrs:
                    setattr(
                        session.client.thread_local,  # type: ignore[union-attr, operator]
                        attr,
                        value,
                    )
                return ff(*args, **kwargs)

            return inner

        return outer

    # Non Spark Connect with SparkSession or Callable
    from pyspark.sql import SparkSession
    from pyspark import SparkContext
    from py4j.clientserver import ClientServer

    if isinstance(SparkContext._gateway, ClientServer):
        # Here's when the pinned-thread mode (PYSPARK_PIN_THREAD) is on.

        if isinstance(f, SparkSession):
            session = f
            assert session is not None
            tags = set(session.getTags())
            # Local properties are copied when wrapping the function.
            assert SparkContext._active_spark_context is not None
            properties = SparkContext._active_spark_context._jsc.sc().getLocalProperties().clone()

            def outer(ff: Callable) -> Callable:
                @functools.wraps(ff)
                def wrapped(*args: Any, **kwargs: Any) -> Any:
                    # Apply properties and tags in the child thread.
                    assert SparkContext._active_spark_context is not None
                    SparkContext._active_spark_context._jsc.sc().setLocalProperties(properties)
                    for tag in tags:
                        session.addTag(tag)  # type: ignore[union-attr]
                    return ff(*args, **kwargs)

                return wrapped

            return outer

        warnings.warn(
            "Spark session is not provided. Tags will not be inherited.",
            UserWarning,
        )

        # NOTICE the internal difference vs `InheritableThread`. `InheritableThread`
        # copies local properties when the thread starts but `inheritable_thread_target`
        # copies when the function is wrapped.
        assert SparkContext._active_spark_context is not None
        properties = SparkContext._active_spark_context._jsc.sc().getLocalProperties().clone()
        assert callable(f)

        @functools.wraps(f)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            # Set local properties in child thread.
            assert SparkContext._active_spark_context is not None
            SparkContext._active_spark_context._jsc.sc().setLocalProperties(properties)
            return f(*args, **kwargs)  # type: ignore[misc, operator]

        return wrapped
    else:
        return f  # type: ignore[return-value]


def handle_worker_exception(
    e: BaseException, outfile: IO, hide_traceback: Optional[bool] = None
) -> None:
    """
    Handles exception for Python worker which writes SpecialLengths.PYTHON_EXCEPTION_THROWN (-2)
    and exception traceback info to outfile. JVM could then read from the outfile and perform
    exception handling there.

    Parameters
    ----------
    e : BaseException
        Exception handled
    outfile : IO
        IO object to write the exception info
    hide_traceback : bool, optional
        Whether to hide the traceback in the output.
        By default, hides the traceback if environment variable SPARK_HIDE_TRACEBACK is set.
    """

    if hide_traceback is None:
        hide_traceback = bool(os.environ.get("SPARK_HIDE_TRACEBACK", False))

    def format_exception() -> str:
        if hide_traceback:
            return "".join(traceback.format_exception_only(type(e), e))
        if os.environ.get("SPARK_SIMPLIFIED_TRACEBACK", False):
            tb = try_simplify_traceback(sys.exc_info()[-1])  # type: ignore[arg-type]
            if tb is not None:
                e.__cause__ = None
                return "".join(traceback.format_exception(type(e), e, tb))
        return traceback.format_exc()

    try:
        exc_info = format_exception()
        write_int(SpecialLengths.PYTHON_EXCEPTION_THROWN, outfile)
        write_with_length(exc_info.encode("utf-8"), outfile)
    except IOError:
        # JVM close the socket
        pass
    except BaseException:
        # Write the error to stderr if it happened while serializing
        print("PySpark worker failed with exception:", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)


class InheritableThread(threading.Thread):
    """
    Thread that is recommended to be used in PySpark when the pinned thread mode is
    enabled. The wrapper function, before calling original thread target, it
    inherits the inheritable properties specific to JVM thread such as
    ``InheritableThreadLocal``, or thread local such as tags
    with Spark Connect.

    When the pinned thread mode is off, this works as :class:`threading.Thread`.

    .. versionadded:: 3.1.0

    .. versionchanged:: 3.5.0
        Supports Spark Connect.

    Notes
    -----
    This API is experimental.
    """

    _props: "JavaObject"

    def __init__(
        self, target: Callable, *args: Any, session: Optional["SparkSession"] = None, **kwargs: Any
    ):
        from pyspark.sql import is_remote

        # Spark Connect
        if is_remote():
            assert session is not None, "Spark Connect must be provided."
            self._session = session

            def copy_local_properties(*a: Any, **k: Any) -> Any:
                # Set tags in child thread.
                assert hasattr(self, "_tags")
                thread_local = session.client.thread_local  # type: ignore[union-attr, operator]
                thread_local.tags = self._tags  # type: ignore[has-type]
                return target(*a, **k)

            super(InheritableThread, self).__init__(
                target=copy_local_properties, *args, **kwargs  # type: ignore[misc]
            )
        else:
            # Non Spark Connect
            from pyspark import SparkContext
            from py4j.clientserver import ClientServer

            self._session = session  # type: ignore[assignment]
            if isinstance(SparkContext._gateway, ClientServer):
                # Here's when the pinned-thread mode (PYSPARK_PIN_THREAD) is on.
                def copy_local_properties(*a: Any, **k: Any) -> Any:
                    # self._props is set before starting the thread to match the behavior with JVM.
                    assert hasattr(self, "_props")
                    if hasattr(self, "_tags"):
                        for tag in self._tags:  # type: ignore[has-type]
                            self._session.addTag(tag)
                    assert SparkContext._active_spark_context is not None
                    SparkContext._active_spark_context._jsc.sc().setLocalProperties(self._props)
                    return target(*a, **k)

                super(InheritableThread, self).__init__(
                    target=copy_local_properties, *args, **kwargs  # type: ignore[misc]
                )
            else:
                super(InheritableThread, self).__init__(
                    target=target, *args, **kwargs  # type: ignore[misc]
                )

    def start(self) -> None:
        from pyspark.sql import is_remote

        if is_remote():
            # Spark Connect
            assert hasattr(self, "_session")
            thread_local = self._session.client.thread_local  # type: ignore[union-attr, operator]
            if not hasattr(thread_local, "tags"):
                thread_local.tags = set()
            self._tags = set(thread_local.tags)
        else:
            # Non Spark Connect
            from pyspark import SparkContext
            from py4j.clientserver import ClientServer

            if isinstance(SparkContext._gateway, ClientServer):
                # Here's when the pinned-thread mode (PYSPARK_PIN_THREAD) is on.

                # Local property copy should happen in Thread.start to mimic JVM's behavior.
                assert SparkContext._active_spark_context is not None
                self._props = (
                    SparkContext._active_spark_context._jsc.sc().getLocalProperties().clone()
                )
                if self._session is not None:
                    self._tags = self._session.getTags()

        return super(InheritableThread, self).start()


class PythonEvalType:
    """
    Evaluation type of python rdd.

    These values are internal to PySpark.

    These values should match values in org.apache.spark.api.python.PythonEvalType.
    """

    NON_UDF: "NonUDFType" = 0

    SQL_BATCHED_UDF: "SQLBatchedUDFType" = 100
    SQL_ARROW_BATCHED_UDF: "SQLArrowBatchedUDFType" = 101

    SQL_SCALAR_PANDAS_UDF: "PandasScalarUDFType" = 200
    SQL_GROUPED_MAP_PANDAS_UDF: "PandasGroupedMapUDFType" = 201
    SQL_GROUPED_AGG_PANDAS_UDF: "PandasGroupedAggUDFType" = 202
    SQL_WINDOW_AGG_PANDAS_UDF: "PandasWindowAggUDFType" = 203
    SQL_SCALAR_PANDAS_ITER_UDF: "PandasScalarIterUDFType" = 204
    SQL_MAP_PANDAS_ITER_UDF: "PandasMapIterUDFType" = 205
    SQL_COGROUPED_MAP_PANDAS_UDF: "PandasCogroupedMapUDFType" = 206
    SQL_MAP_ARROW_ITER_UDF: "ArrowMapIterUDFType" = 207
    SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE: "PandasGroupedMapUDFWithStateType" = 208
    SQL_GROUPED_MAP_ARROW_UDF: "ArrowGroupedMapUDFType" = 209
    SQL_COGROUPED_MAP_ARROW_UDF: "ArrowCogroupedMapUDFType" = 210
    SQL_TRANSFORM_WITH_STATE_PANDAS_UDF: "PandasGroupedMapUDFTransformWithStateType" = 211
    SQL_TRANSFORM_WITH_STATE_PANDAS_INIT_STATE_UDF: "PandasGroupedMapUDFTransformWithStateInitStateType" = (  # noqa: E501
        212
    )
    SQL_TRANSFORM_WITH_STATE_PYTHON_ROW_UDF: "GroupedMapUDFTransformWithStateType" = 213
    SQL_TRANSFORM_WITH_STATE_PYTHON_ROW_INIT_STATE_UDF: "GroupedMapUDFTransformWithStateInitStateType" = (  # noqa: E501
        214
    )
    SQL_TABLE_UDF: "SQLTableUDFType" = 300
    SQL_ARROW_TABLE_UDF: "SQLArrowTableUDFType" = 301


def _create_local_socket(sock_info: "JavaArray") -> "io.BufferedRWPair":
    """
    Create a local socket that can be used to load deserialized data from the JVM

    Parameters
    ----------
    sock_info : tuple
        Tuple containing port number and authentication secret for a local socket.

    Returns
    -------
    sockfile file descriptor of the local socket
    """
    sockfile: "io.BufferedRWPair"
    sock: "socket.socket"
    conn_info: int = sock_info[0]
    auth_secret: str = sock_info[1]
    sockfile, sock = local_connect_and_auth(conn_info, auth_secret)
    # The RDD materialization time is unpredictable, if we set a timeout for socket reading
    # operation, it will very possibly fail. See SPARK-18281.
    sock.settimeout(None)
    return sockfile


def _load_from_socket(sock_info: "JavaArray", serializer: "Serializer") -> Iterator[Any]:
    """
    Connect to a local socket described by sock_info and use the given serializer to yield data

    Parameters
    ----------
    sock_info : tuple
        Tuple containing port number and authentication secret for a local socket.
    serializer : class:`Serializer`
        The PySpark serializer to use

    Returns
    -------
    result of meth:`Serializer.load_stream`,
    usually a generator that yields deserialized data
    """
    sockfile = _create_local_socket(sock_info)
    # The socket will be automatically closed when garbage-collected.
    return serializer.load_stream(sockfile)


def _local_iterator_from_socket(sock_info: "JavaArray", serializer: "Serializer") -> Iterator[Any]:
    class PyLocalIterable:
        """Create a synchronous local iterable over a socket"""

        def __init__(self, _sock_info: "JavaArray", _serializer: "Serializer"):
            port: int
            auth_secret: str
            jsocket_auth_server: "JavaObject"
            port, auth_secret, self.jsocket_auth_server = _sock_info
            self._sockfile = _create_local_socket((port, auth_secret))
            self._serializer = _serializer
            self._read_iter: Iterator[Any] = iter([])  # Initialize as empty iterator
            self._read_status = 1

        def __iter__(self) -> Iterator[Any]:
            while self._read_status == 1:
                # Request next partition data from Java
                write_int(1, self._sockfile)
                self._sockfile.flush()

                # If response is 1 then there is a partition to read, if 0 then fully consumed
                self._read_status = read_int(self._sockfile)
                if self._read_status == 1:
                    # Load the partition data as a stream and read each item
                    self._read_iter = self._serializer.load_stream(self._sockfile)
                    for item in self._read_iter:
                        yield item

                # An error occurred, join serving thread and raise any exceptions from the JVM
                elif self._read_status == -1:
                    self.jsocket_auth_server.getResult()

        def __del__(self) -> None:
            # If local iterator is not fully consumed,
            if self._read_status == 1:
                try:
                    # Finish consuming partition data stream
                    for _ in self._read_iter:
                        pass
                    # Tell Java to stop sending data and close connection
                    write_int(0, self._sockfile)
                    self._sockfile.flush()
                except Exception:
                    # Ignore any errors, socket is automatically closed when garbage-collected
                    pass

    return iter(PyLocalIterable(sock_info, serializer))


def local_connect_and_auth(
    conn_info: Optional[Union[str, int]], auth_secret: Optional[str]
) -> Tuple:
    """
    Connect to local host, authenticate with it, and return a (sockfile,sock) for that connection.
    Handles IPV4 & IPV6, does some error handling.

    Parameters
    ----------
    port : str or int, optional
    auth_secret : str, optional

    Returns
    -------
    tuple
        with (sockfile, sock)
    """
    is_unix_domain_socket = isinstance(conn_info, str) and auth_secret is None
    if is_unix_domain_socket:
        sock_path = conn_info
        assert isinstance(sock_path, str)
        sock = None
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(int(os.environ.get("SPARK_AUTH_SOCKET_TIMEOUT", 15)))
            sock.connect(sock_path)
            sockfile = sock.makefile("rwb", int(os.environ.get("SPARK_BUFFER_SIZE", 65536)))
            return (sockfile, sock)
        except socket.error as e:
            if sock is not None:
                sock.close()
            raise PySparkRuntimeError(
                errorClass="CANNOT_OPEN_SOCKET",
                messageParameters={
                    "errors": "tried to connect to %s, but an error occurred: %s"
                    % (sock_path, str(e)),
                },
            )

    sock = None
    errors = []
    # Support for both IPv4 and IPv6.
    addr = "127.0.0.1"
    if os.environ.get("SPARK_PREFER_IPV6", "false").lower() == "true":
        addr = "::1"
    for res in socket.getaddrinfo(addr, conn_info, socket.AF_UNSPEC, socket.SOCK_STREAM):
        af, socktype, proto, _, sa = res
        try:
            sock = socket.socket(af, socktype, proto)
            sock.settimeout(int(os.environ.get("SPARK_AUTH_SOCKET_TIMEOUT", 15)))
            sock.connect(sa)
            sockfile = sock.makefile("rwb", int(os.environ.get("SPARK_BUFFER_SIZE", 65536)))
            assert isinstance(auth_secret, str)
            _do_server_auth(sockfile, auth_secret)
            return (sockfile, sock)
        except socket.error as e:
            emsg = str(e)
            errors.append("tried to connect to %s, but an error occurred: %s" % (sa, emsg))
            if sock is not None:
                sock.close()
                sock = None
    raise PySparkRuntimeError(
        errorClass="CANNOT_OPEN_SOCKET",
        messageParameters={
            "errors": str(errors),
        },
    )


def _do_server_auth(conn: "io.IOBase", auth_secret: str) -> None:
    """
    Performs the authentication protocol defined by the SocketAuthHelper class on the given
    file-like object 'conn'.
    """
    write_with_length(auth_secret.encode("utf-8"), conn)
    conn.flush()
    reply = UTF8Deserializer().loads(conn)
    if reply != "ok":
        conn.close()
        raise PySparkRuntimeError(
            errorClass="UNEXPECTED_RESPONSE_FROM_SERVER",
            messageParameters={},
        )


_is_remote_only = None


def is_remote_only() -> bool:
    """
    Returns if the current running environment is only for Spark Connect.
    If users install pyspark-client alone, RDD API does not exist.

    .. versionadded:: 4.0.0

    Notes
    -----
    This will only return ``True`` if installed PySpark is only for Spark Connect.
    Otherwise, it returns ``False``.

    This API is unstable, and for developers.

    Returns
    -------
    bool

    Examples
    --------
    >>> from pyspark.sql import is_remote
    >>> is_remote()
    False
    """
    global _is_remote_only

    if "SPARK_SKIP_CONNECT_COMPAT_TESTS" in os.environ:
        return True

    if _is_remote_only is not None:
        return _is_remote_only
    try:
        from pyspark import core  # noqa: F401

        _is_remote_only = False
        return _is_remote_only
    except ImportError:
        _is_remote_only = True
        return _is_remote_only


# This function will be called in `pyspark` script as well,
# so this should be available without a running Spark.
# If move or rename, please update the script too.
def spark_connect_mode() -> str:
    """
    Return the env var SPARK_CONNECT_MODE; otherwise "1" if `pyspark_connect` is available.
    """
    connect_by_default = os.environ.get("SPARK_CONNECT_MODE")
    if connect_by_default is not None:
        return connect_by_default
    try:
        import pyspark_connect  # noqa: F401

        return "1"
    except ImportError:
        return "0"


def default_api_mode() -> str:
    """
    Return the default API mode.
    """
    if spark_connect_mode() == "1":
        return "connect"
    else:
        return "classic"


if __name__ == "__main__":
    if "pypy" not in platform.python_implementation().lower() and sys.version_info[:2] >= (3, 9):
        import doctest
        import pyspark.util
        from pyspark.core.context import SparkContext

        globs = pyspark.util.__dict__.copy()
        globs["sc"] = SparkContext("local[4]", "PythonTest")
        (failure_count, test_count) = doctest.testmod(pyspark.util, globs=globs)
        globs["sc"].stop()

        if failure_count:
            sys.exit(-1)
