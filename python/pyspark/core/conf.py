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

__all__ = ["SparkConf"]

import sys
from typing import Dict, List, Optional, Tuple, cast, overload

from py4j.java_gateway import JVMView, JavaObject

from pyspark.errors import PySparkRuntimeError


class SparkConf:
    """
    Configuration for a Spark application. Used to set various Spark
    parameters as key-value pairs.

    Most of the time, you would create a SparkConf object with
    ``SparkConf()``, which will load values from `spark.*` Java system
    properties as well. In this case, any parameters you set directly on
    the :class:`SparkConf` object take priority over system properties.

    For unit tests, you can also call ``SparkConf(false)`` to skip
    loading external settings and get the same configuration no matter
    what the system properties are.

    All setter methods in this class support chaining. For example,
    you can write ``conf.setMaster("local").setAppName("My app")``.

    Parameters
    ----------
    loadDefaults : bool
        whether to load values from Java system properties (True by default)
    _jvm : class:`py4j.java_gateway.JVMView`
        internal parameter used to pass a handle to the
        Java VM; does not need to be set by users
    _jconf : class:`py4j.java_gateway.JavaObject`
        Optionally pass in an existing SparkConf handle
        to use its parameters

    Notes
    -----
    Once a SparkConf object is passed to Spark, it is cloned
    and can no longer be modified by the user.

    Examples
    --------
    >>> from pyspark.core.conf import SparkConf
    >>> from pyspark.core.context import SparkContext
    >>> conf = SparkConf()
    >>> conf.setMaster("local").setAppName("My app")
    <pyspark.core.conf.SparkConf object at ...>
    >>> conf.get("spark.master")
    'local'
    >>> conf.get("spark.app.name")
    'My app'
    >>> sc = SparkContext(conf=conf)
    >>> sc.master
    'local'
    >>> sc.appName
    'My app'
    >>> sc.sparkHome is None
    True

    >>> conf = SparkConf(loadDefaults=False)
    >>> conf.setSparkHome("/path")
    <pyspark.core.conf.SparkConf object at ...>
    >>> conf.get("spark.home")
    '/path'
    >>> conf.setExecutorEnv("VAR1", "value1")
    <pyspark.core.conf.SparkConf object at ...>
    >>> conf.setExecutorEnv(pairs = [("VAR3", "value3"), ("VAR4", "value4")])
    <pyspark.core.conf.SparkConf object at ...>
    >>> conf.get("spark.executorEnv.VAR1")
    'value1'
    >>> print(conf.toDebugString())
    spark.executorEnv.VAR1=value1
    spark.executorEnv.VAR3=value3
    spark.executorEnv.VAR4=value4
    spark.home=/path
    >>> for p in sorted(conf.getAll(), key=lambda p: p[0]):
    ...     print(p)
    ('spark.executorEnv.VAR1', 'value1')
    ('spark.executorEnv.VAR3', 'value3')
    ('spark.executorEnv.VAR4', 'value4')
    ('spark.home', '/path')
    >>> conf._jconf.setExecutorEnv("VAR5", "value5")
    JavaObject id...
    >>> print(conf.toDebugString())
    spark.executorEnv.VAR1=value1
    spark.executorEnv.VAR3=value3
    spark.executorEnv.VAR4=value4
    spark.executorEnv.VAR5=value5
    spark.home=/path
    """

    _jconf: Optional[JavaObject]
    _conf: Optional[Dict[str, str]]

    def __init__(
        self,
        loadDefaults: bool = True,
        _jvm: Optional[JVMView] = None,
        _jconf: Optional[JavaObject] = None,
    ):
        """
        Create a new Spark configuration.
        """
        if _jconf:
            self._jconf = _jconf
        else:
            from pyspark.core.context import SparkContext

            _jvm = _jvm or SparkContext._jvm

            if _jvm is not None:
                # JVM is created, so create self._jconf directly through JVM
                self._jconf = _jvm.SparkConf(loadDefaults)
                self._conf = None
            else:
                # JVM is not created, so store data in self._conf first
                self._jconf = None
                self._conf = {}

    def set(self, key: str, value: str) -> "SparkConf":
        """Set a configuration property."""
        # Try to set self._jconf first if JVM is created, set self._conf if JVM is not created yet.
        if self._jconf is not None:
            self._jconf.set(key, str(value))
        else:
            assert self._conf is not None
            self._conf[key] = str(value)
        return self

    def setIfMissing(self, key: str, value: str) -> "SparkConf":
        """Set a configuration property, if not already set."""
        if self.get(key) is None:
            self.set(key, value)
        return self

    def setMaster(self, value: str) -> "SparkConf":
        """Set master URL to connect to."""
        self.set("spark.master", value)
        return self

    def setAppName(self, value: str) -> "SparkConf":
        """Set application name."""
        self.set("spark.app.name", value)
        return self

    def setSparkHome(self, value: str) -> "SparkConf":
        """Set path where Spark is installed on worker nodes."""
        self.set("spark.home", value)
        return self

    @overload
    def setExecutorEnv(self, key: str, value: str) -> "SparkConf":
        ...

    @overload
    def setExecutorEnv(self, *, pairs: List[Tuple[str, str]]) -> "SparkConf":
        ...

    def setExecutorEnv(
        self,
        key: Optional[str] = None,
        value: Optional[str] = None,
        pairs: Optional[List[Tuple[str, str]]] = None,
    ) -> "SparkConf":
        """Set an environment variable to be passed to executors."""
        if (key is not None and pairs is not None) or (key is None and pairs is None):
            raise PySparkRuntimeError(
                error_class="KEY_VALUE_PAIR_REQUIRED",
                message_parameters={},
            )
        elif key is not None:
            self.set("spark.executorEnv.{}".format(key), cast(str, value))
        elif pairs is not None:
            for k, v in pairs:
                self.set("spark.executorEnv.{}".format(k), v)
        return self

    def setAll(self, pairs: List[Tuple[str, str]]) -> "SparkConf":
        """
        Set multiple parameters, passed as a list of key-value pairs.

        Parameters
        ----------
        pairs : iterable of tuples
            list of key-value pairs to set
        """
        for k, v in pairs:
            self.set(k, v)
        return self

    @overload
    def get(self, key: str) -> Optional[str]:
        ...

    @overload
    def get(self, key: str, defaultValue: None) -> Optional[str]:
        ...

    @overload
    def get(self, key: str, defaultValue: str) -> str:
        ...

    def get(self, key: str, defaultValue: Optional[str] = None) -> Optional[str]:
        """Get the configured value for some key, or return a default otherwise."""
        if defaultValue is None:  # Py4J doesn't call the right get() if we pass None
            if self._jconf is not None:
                if not self._jconf.contains(key):
                    return None
                return self._jconf.get(key)
            else:
                assert self._conf is not None
                return self._conf.get(key, None)
        else:
            if self._jconf is not None:
                return self._jconf.get(key, defaultValue)
            else:
                assert self._conf is not None
                return self._conf.get(key, defaultValue)

    def getAll(self) -> List[Tuple[str, str]]:
        """Get all values as a list of key-value pairs."""
        if self._jconf is not None:
            return [(elem._1(), elem._2()) for elem in cast(JavaObject, self._jconf).getAll()]
        else:
            assert self._conf is not None
            return list(self._conf.items())

    def contains(self, key: str) -> bool:
        """Does this configuration contain a given key?"""
        if self._jconf is not None:
            return self._jconf.contains(key)
        else:
            assert self._conf is not None
            return key in self._conf

    def toDebugString(self) -> str:
        """
        Returns a printable version of the configuration, as a list of
        key=value pairs, one per line.
        """
        if self._jconf is not None:
            return self._jconf.toDebugString()
        else:
            assert self._conf is not None
            return "\n".join("%s=%s" % (k, v) for k, v in self._conf.items())


def _test() -> None:
    import doctest

    (failure_count, test_count) = doctest.testmod(optionflags=doctest.ELLIPSIS)
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
