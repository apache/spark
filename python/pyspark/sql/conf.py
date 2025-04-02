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

import sys
from typing import Any, Dict, Optional, Union, TYPE_CHECKING

from pyspark import _NoValue
from pyspark._globals import _NoValueType
from pyspark.errors import PySparkTypeError

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
