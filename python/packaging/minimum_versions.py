#!/usr/bin/env python3

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

"""
Parse minimum dependency versions from dev/lock-files/pyspark-deps.in.

That file is the single source of truth for PySpark's runtime dependency
floor versions.  All three packaging setup.py files import from here so
version bumps only need to happen in one place.
"""

import os
import re

# Called from spark/python as cwd (the setup.py files all chdir there).
_DEPS_IN = os.path.join(
    os.path.dirname(__file__), "..", "..", "dev", "lock-files", "pyspark-deps.in"
)


def _parse_minimum_version(package_name: str) -> str:
    """
    Extract the minimum version for *package_name* from pyspark-deps.in.

    Matches lines like:
        pandas>=2.2.0
        grpcio>=1.76.0
        py4j>=0.10.9.7,<0.10.9.10
    Returns the version string after the first `>=`.
    Raises RuntimeError if the package is not found.
    """
    pattern = re.compile(
        r"^\s*" + re.escape(package_name) + r"\s*>=\s*([^\s,;\\]+)",
        re.IGNORECASE,
    )
    deps_in = os.path.abspath(_DEPS_IN)
    with open(deps_in) as f:
        for line in f:
            m = pattern.match(line)
            if m:
                return m.group(1)
    raise RuntimeError(f"Could not find minimum version for '{package_name}' in {deps_in}")


minimum_pandas_version = _parse_minimum_version("pandas")
minimum_numpy_version = _parse_minimum_version("numpy")
minimum_pyarrow_version = _parse_minimum_version("pyarrow")
minimum_grpc_version = _parse_minimum_version("grpcio")
minimum_googleapis_common_protos_version = _parse_minimum_version("googleapis-common-protos")
minimum_pyyaml_version = _parse_minimum_version("pyyaml")
minimum_zstandard_version = _parse_minimum_version("zstandard")
