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
from pyspark.errors import PySparkValueError, PySparkTypeError
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

from typing import Any, Dict, Optional, Union, cast
import warnings

from pyspark import _NoValue
from pyspark._globals import _NoValueType
from pyspark.sql.conf import RuntimeConfig as PySparkRuntimeConfig
from pyspark.sql.connect import proto
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
        op_set = proto.ConfigRequest.Set(pairs=[proto.KeyValue(key=key, value=value)])
        operation = proto.ConfigRequest.Operation(set=op_set)
        result = self._client.config(operation)
        for warn in result.warnings:
            warnings.warn(warn)

    set.__doc__ = PySparkRuntimeConfig.set.__doc__

    def get(
        self, key: str, default: Union[Optional[str], _NoValueType] = _NoValue
    ) -> Optional[str]:
        self._checkType(key, "key")
        if default is _NoValue:
            op_get = proto.ConfigRequest.Get(keys=[key])
            operation = proto.ConfigRequest.Operation(get=op_get)
        else:
            if default is not None:
                self._checkType(default, "default")
            op_get_with_default = proto.ConfigRequest.GetWithDefault(
                pairs=[proto.KeyValue(key=key, value=cast(Optional[str], default))]
            )
            operation = proto.ConfigRequest.Operation(get_with_default=op_get_with_default)
        result = self._client.config(operation)
        return result.pairs[0][1]

    get.__doc__ = PySparkRuntimeConfig.get.__doc__

    @property
    def getAll(self) -> Dict[str, str]:
        op_get_all = proto.ConfigRequest.GetAll()
        operation = proto.ConfigRequest.Operation(get_all=op_get_all)
        result = self._client.config(operation)
        confs: Dict[str, str] = dict()
        for key, value in result.pairs:
            assert value is not None
            confs[key] = value
        return confs

    getAll.__doc__ = PySparkRuntimeConfig.getAll.__doc__

    def unset(self, key: str) -> None:
        op_unset = proto.ConfigRequest.Unset(keys=[key])
        operation = proto.ConfigRequest.Operation(unset=op_unset)
        result = self._client.config(operation)
        for warn in result.warnings:
            warnings.warn(warn)

    unset.__doc__ = PySparkRuntimeConfig.unset.__doc__

    def isModifiable(self, key: str) -> bool:
        op_is_modifiable = proto.ConfigRequest.IsModifiable(keys=[key])
        operation = proto.ConfigRequest.Operation(is_modifiable=op_is_modifiable)
        result = self._client.config(operation).pairs[0][1]
        if result == "true":
            return True
        elif result == "false":
            return False
        else:
            raise PySparkValueError(
                error_class="VALUE_NOT_ALLOWED",
                message_parameters={"arg_name": "result", "allowed_values": "'true' or 'false'"},
            )

    isModifiable.__doc__ = PySparkRuntimeConfig.isModifiable.__doc__

    def _checkType(self, obj: Any, identifier: str) -> None:
        """Assert that an object is of type str."""
        if not isinstance(obj, str):
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={
                    "arg_name": identifier,
                    "arg_type": type(obj).__name__,
                },
            )


RuntimeConf.__doc__ = PySparkRuntimeConfig.__doc__


def _test() -> None:
    import os
    import sys
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.connect.conf

    globs = pyspark.sql.connect.conf.__dict__.copy()
    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.conf tests")
        .remote(os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local[4]"))
        .getOrCreate()
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
