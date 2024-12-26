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
from functools import cached_property
import sys
from typing import Any, Dict, Optional, Union, TYPE_CHECKING, List, cast

from pyspark import _NoValue
from pyspark._globals import _NoValueType
from pyspark.errors import PySparkTypeError, SparkNoSuchElementException
from pyspark.logger import PySparkLogger
from pyspark.sql.utils import get_active_spark_context

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject


class RuntimeConfig:
    """User-facing configuration API, accessible through `SparkSession.conf`.

    Options set here are automatically propagated to the Hadoop configuration during I/O.

    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    """

    def __init__(self, jconf: "JavaObject") -> None:
        """Create a new RuntimeConfig that wraps the underlying JVM object."""
        self._jconf = jconf

    def set(self, key: str, value: Union[str, int, bool]) -> None:
        """
        Sets the given Spark runtime configuration property.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        key : str
            key of the configuration to set.
        value : str, int, or bool
            value of the configuration to set.

        Examples
        --------
        >>> spark.conf.set("key1", "value1")
        """
        self._jconf.set(key, value)

    def get(
        self, key: str, default: Union[Optional[str], _NoValueType] = _NoValue
    ) -> Optional[str]:
        """
        Returns the value of Spark runtime configuration property for the given key,
        assuming it is set.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        key : str
            key of the configuration to get.
        default : str, optional
            value of the configuration to get if the key does not exist.

        Returns
        -------
        The string value of the configuration set, or None.

        Examples
        --------
        >>> spark.conf.get("non-existent-key", "my_default")
        'my_default'
        >>> spark.conf.set("my_key", "my_value")
        >>> spark.conf.get("my_key")
        'my_value'
        """
        self._check_type(key, "key")
        if default is _NoValue:
            return self._jconf.get(key)
        else:
            if default is not None:
                self._check_type(default, "default")
            return self._jconf.get(key, default)

    @property
    def getAll(self) -> Dict[str, str]:
        """
        Returns all properties set in this conf.

        .. versionadded:: 4.0.0

        Returns
        -------
        dict
            A dictionary containing all properties set in this conf.
        """
        return dict(self._jconf.getAllAsJava())

    def unset(self, key: str) -> None:
        """
        Resets the configuration property for the given key.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        key : str
            key of the configuration to unset.

        Examples
        --------
        >>> spark.conf.set("my_key", "my_value")
        >>> spark.conf.get("my_key")
        'my_value'
        >>> spark.conf.unset("my_key")
        >>> spark.conf.get("my_key")
        Traceback (most recent call last):
           ...
        pyspark...SparkNoSuchElementException: ... The SQL config "my_key" cannot be found...
        """
        self._jconf.unset(key)

    def _check_type(self, obj: Any, identifier: str) -> None:
        """Assert that an object is of type str."""
        if not isinstance(obj, str):
            raise PySparkTypeError(
                errorClass="NOT_STR",
                messageParameters={
                    "arg_name": identifier,
                    "arg_type": type(obj).__name__,
                },
            )

    def isModifiable(self, key: str) -> bool:
        """Indicates whether the configuration property with the given key
        is modifiable in the current session.

        .. versionadded:: 2.4.0
        """
        return self._jconf.isModifiable(key)

    @cached_property
    def spark(self) -> "RuntimeConfigDictWrapper":
        from py4j.java_gateway import JVMView

        sc = get_active_spark_context()
        jvm = cast(JVMView, sc._jvm)
        d = {}
        for entry in jvm.PythonSQLUtils.listAllSQLConfigs():
            k = entry._1()
            default = entry._2()
            doc = entry._3()
            ver = entry._4()
            entry = SQLConfEntry(k, default, doc, ver)
            entry.__doc__ = doc  # So help function work
            d[k] = entry
        return RuntimeConfigDictWrapper(self, d, prefix="spark")

    def __setitem__(self, key: Any, val: Any) -> None:
        if key.startswith("spark."):
            self.spark[key[6:]] = val
        else:
            super().__setattr__(key, val)

    def __getitem__(self, item: Any) -> Union["RuntimeConfigDictWrapper", str]:
        if item.startswith("spark."):
            return self.spark[item[6:]]
        else:
            return object.__getattribute__(self, item)


class SQLConfEntry(str):
    def __new__(cls, name: str, value: str, description: str, version: str) -> "SQLConfEntry":
        return super().__new__(cls, value)

    def __init__(self, name: str, value: str, description: str, version: str):
        self._name = name
        self._value = value
        self._description = description
        self._version = version

    def desc(self) -> str:
        return self._description

    def version(self) -> str:
        return self._version


class RuntimeConfigDictWrapper:
    """provide attribute-style access to a nested dict"""

    _logger = PySparkLogger.getLogger("RuntimeConfigDictWrapper")

    def __init__(self, conf: RuntimeConfig, d: Dict[str, SQLConfEntry], prefix: str = ""):
        object.__setattr__(self, "d", d)
        object.__setattr__(self, "prefix", prefix)
        object.__setattr__(self, "_conf", conf)

    def __setattr__(self, key: str, val: Any) -> None:
        prefix = object.__getattribute__(self, "prefix")
        d = object.__getattribute__(self, "d")
        if prefix:
            prefix += "."
        canonical_key = prefix + key

        candidates = [
            k for k in d.keys() if all(x in k.split(".") for x in canonical_key.split("."))
        ]
        if len(candidates) == 0:
            RuntimeConfigDictWrapper._logger.info(
                "Setting a configuration '{}' to '{}' (non built-in configuration).".format(
                    canonical_key, val
                )
            )
        object.__getattribute__(self, "_conf").set(canonical_key, val)

    __setitem__ = __setattr__

    def __getattr__(self, key: str) -> Union["RuntimeConfigDictWrapper", str]:
        prefix = object.__getattribute__(self, "prefix")
        d = object.__getattribute__(self, "d")
        conf = object.__getattribute__(self, "_conf")
        if prefix:
            prefix += "."
        canonical_key = prefix + key

        try:
            value = conf.get(canonical_key)
            description = "Documentation not found for '{}'.".format(canonical_key)
            version = "Version not found for '{}'.".format(canonical_key)
            if canonical_key in d:
                description = d[canonical_key]._description
                version = d[canonical_key]._version

            return SQLConfEntry(canonical_key, value, description, version)
        except SparkNoSuchElementException:
            if not prefix.startswith("_"):
                return RuntimeConfigDictWrapper(conf, d, canonical_key)
            raise

    __getitem__ = __getattr__

    def __dir__(self) -> List[str]:
        prefix = object.__getattribute__(self, "prefix")
        d = object.__getattribute__(self, "d")

        if prefix == "":
            candidates = d.keys()
            offset = 0
        else:
            candidates = [k for k in d.keys() if all(x in k.split(".") for x in prefix.split("."))]
            offset = len(prefix) + 1  # prefix (e.g. "spark.") to trim.
        return [c[offset:] for c in candidates]


def _test() -> None:
    import os
    import doctest
    from pyspark.sql.session import SparkSession
    import pyspark.sql.conf

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.conf.__dict__.copy()
    spark = SparkSession.builder.master("local[4]").appName("sql.conf tests").getOrCreate()
    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.conf, globs=globs, optionflags=doctest.ELLIPSIS
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
