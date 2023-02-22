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
from typing import Any, Optional, Union, cast
import warnings

from pyspark import _NoValue
from pyspark._globals import _NoValueType
from pyspark.sql.conf import RuntimeConfig as PySparkRuntimeConfig
from pyspark.sql.connect.client import SparkConnectClient


class RuntimeConf:
    def __init__(self, client: SparkConnectClient) -> None:
        """Create a new RuntimeConfig."""
        self._client = client

    __init__.__doc__ = PySparkRuntimeConfig.__init__.__doc__

    def set(self, key: str, value: Union[str, int, bool]) -> None:
        if isinstance(value, bool):
            value = "true" if value else "false"
        elif isinstance(value, int):
            value = str(value)
        result = self._client.config("set", keys=[key], optional_values=[value])
        for warn in result.warnings:
            warnings.warn(warn)

    set.__doc__ = PySparkRuntimeConfig.set.__doc__

    def get(
        self, key: str, default: Union[Optional[str], _NoValueType] = _NoValue
    ) -> Optional[str]:
        self._checkType(key, "key")
        if default is _NoValue:
            return self._client.config("get", keys=[key]).values[0]
        else:
            if default is not None:
                self._checkType(default, "default")
            return self._client.config(
                "get", keys=[key], optional_values=[cast(Optional[str], default)]
            ).optional_values[0]

    get.__doc__ = PySparkRuntimeConfig.get.__doc__

    def unset(self, key: str) -> None:
        result = self._client.config("unset", keys=[key])
        for warn in result.warnings:
            warnings.warn(warn)

    unset.__doc__ = PySparkRuntimeConfig.unset.__doc__

    def isModifiable(self, key: str) -> bool:
        return self._client.config("is_modifiable", keys=[key]).bools[0]

    isModifiable.__doc__ = PySparkRuntimeConfig.isModifiable.__doc__

    def _checkType(self, obj: Any, identifier: str) -> None:
        """Assert that an object is of type str."""
        if not isinstance(obj, str):
            raise TypeError(
                "expected %s '%s' to be a string (was '%s')" % (identifier, obj, type(obj).__name__)
            )


RuntimeConf.__doc__ = PySparkRuntimeConfig.__doc__


def _test() -> None:
    import sys
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.connect.conf

    globs = pyspark.sql.connect.conf.__dict__.copy()
    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.conf tests").remote("local[4]").getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.connect.conf,
        globs=globs,
        optionflags=doctest.ELLIPSIS
        | doctest.NORMALIZE_WHITESPACE
        | doctest.IGNORE_EXCEPTION_DETAIL,
    )

    globs["spark"].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
