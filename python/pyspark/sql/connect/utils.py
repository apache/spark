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
from typing import Optional, Sequence, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession

from pyspark.loose_version import LooseVersion
from pyspark.sql.pandas.utils import require_minimum_pandas_version, require_minimum_pyarrow_version
from pyspark.errors import PySparkImportError


def check_dependencies(mod_name: str) -> None:
    if mod_name == "__main__" or mod_name == "pyspark.sql.connect.utils":
        from pyspark.testing.connectutils import should_test_connect, connect_requirement_message

        if not should_test_connect:
            print(
                f"Skipping {mod_name} doctests: {connect_requirement_message}",
                file=sys.stderr,
            )
            sys.exit(0)
    else:
        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
        require_minimum_grpc_version()
        require_minimum_grpcio_status_version()
        require_minimum_googleapis_common_protos_version()


def require_minimum_grpc_version() -> None:
    """Raise ImportError if minimum version of grpc is not installed"""
    minimum_grpc_version = "1.48.1"

    try:
        import grpc
    except ImportError as error:
        raise PySparkImportError(
            errorClass="PACKAGE_NOT_INSTALLED",
            messageParameters={
                "package_name": "grpcio",
                "minimum_version": str(minimum_grpc_version),
            },
        ) from error
    if LooseVersion(grpc.__version__) < LooseVersion(minimum_grpc_version):
        raise PySparkImportError(
            errorClass="UNSUPPORTED_PACKAGE_VERSION",
            messageParameters={
                "package_name": "grpcio",
                "minimum_version": str(minimum_grpc_version),
                "current_version": str(grpc.__version__),
            },
        )


def require_minimum_grpcio_status_version() -> None:
    """Raise ImportError if grpcio-status is not installed"""
    minimum_grpc_version = "1.48.1"

    try:
        import grpc_status  # noqa
    except ImportError as error:
        raise PySparkImportError(
            errorClass="PACKAGE_NOT_INSTALLED",
            messageParameters={
                "package_name": "grpcio-status",
                "minimum_version": str(minimum_grpc_version),
            },
        ) from error


def require_minimum_googleapis_common_protos_version() -> None:
    """Raise ImportError if googleapis-common-protos is not installed"""
    minimum_common_protos_version = "1.56.4"

    try:
        import google.rpc  # noqa
    except ImportError as error:
        raise PySparkImportError(
            errorClass="PACKAGE_NOT_INSTALLED",
            messageParameters={
                "package_name": "googleapis-common-protos",
                "minimum_version": str(minimum_common_protos_version),
            },
        ) from error


def get_python_ver() -> str:
    return "%d.%d" % sys.version_info[:2]


class LazyConfigGetter:
    def __init__(
        self,
        keys: Sequence[str],
        session: "SparkSession",
    ):
        assert len(keys) > 0 and len(keys) == len(set(keys))
        assert all(isinstance(key, str) for key in keys)
        assert session is not None
        self._keys = keys
        self._session = session
        self._values: Dict[str, Optional[str]] = {}

    def __getitem__(self, key: str) -> Optional[str]:
        assert key in self._keys

        if len(self._values) == 0:
            values = self._session._client.get_configs(*self._keys)
            for i, value in enumerate(values):
                self._values[self._keys[i]] = value

        return self._values[key]
