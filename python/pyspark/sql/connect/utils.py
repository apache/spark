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

from pyspark.sql.pandas.utils import require_minimum_pandas_version, require_minimum_pyarrow_version


def check_dependencies(mod_name: str) -> None:
    if mod_name == "__main__":
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


def require_minimum_grpc_version() -> None:
    """Raise ImportError if minimum version of grpc is not installed"""
    minimum_grpc_version = "1.48.1"

    from distutils.version import LooseVersion

    try:
        import grpc
    except ImportError as error:
        raise ImportError(
            "grpc >= %s must be installed; however, " "it was not found." % minimum_grpc_version
        ) from error
    if LooseVersion(grpc.__version__) < LooseVersion(minimum_grpc_version):
        raise ImportError(
            "gRPC >= %s must be installed; however, "
            "your version was %s." % (minimum_grpc_version, grpc.__version__)
        )
