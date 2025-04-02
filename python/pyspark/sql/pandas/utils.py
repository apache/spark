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

from pyspark.loose_version import LooseVersion
from pyspark.errors import PySparkImportError, PySparkRuntimeError


def require_minimum_pandas_version() -> None:
    """Raise ImportError if minimum version of Pandas is not installed"""
    # TODO(HyukjinKwon): Relocate and deduplicate the version specification.
    minimum_pandas_version = "2.0.0"

    try:
        import pandas

        # Even if pandas is deleted, if the pandas extension package (e.g. pandas-stubs) is still
        # installed, the pandas path will not be completely deleted.
        # Therefore, even if the import is successful, additional check is required here to verify
        # that pandas is actually installed properly.
        if hasattr(pandas, "__version__"):
            have_pandas = True
        else:
            have_pandas = False
            raised_error = None
    except ImportError as error:
        have_pandas = False
        raised_error = error
    if not have_pandas:
        raise PySparkImportError(
            errorClass="PACKAGE_NOT_INSTALLED",
            messageParameters={
                "package_name": "Pandas",
                "minimum_version": str(minimum_pandas_version),
            },
        ) from raised_error
    if LooseVersion(pandas.__version__) < LooseVersion(minimum_pandas_version):
        raise PySparkImportError(
            errorClass="UNSUPPORTED_PACKAGE_VERSION",
            messageParameters={
                "package_name": "Pandas",
                "minimum_version": str(minimum_pandas_version),
                "current_version": str(pandas.__version__),
            },
        )


def require_minimum_pyarrow_version() -> None:
    """Raise ImportError if minimum version of pyarrow is not installed"""
    # TODO(HyukjinKwon): Relocate and deduplicate the version specification.
    minimum_pyarrow_version = "11.0.0"

    import os

    try:
        import pyarrow

        have_arrow = True
    except ImportError as error:
        have_arrow = False
        raised_error = error
    if not have_arrow:
        raise PySparkImportError(
            errorClass="PACKAGE_NOT_INSTALLED",
            messageParameters={
                "package_name": "PyArrow",
                "minimum_version": str(minimum_pyarrow_version),
            },
        ) from raised_error
    if LooseVersion(pyarrow.__version__) < LooseVersion(minimum_pyarrow_version):
        raise PySparkImportError(
            errorClass="UNSUPPORTED_PACKAGE_VERSION",
            messageParameters={
                "package_name": "PyArrow",
                "minimum_version": str(minimum_pyarrow_version),
                "current_version": str(pyarrow.__version__),
            },
        )
    if os.environ.get("ARROW_PRE_0_15_IPC_FORMAT", "0") == "1":
        raise PySparkRuntimeError(
            errorClass="ARROW_LEGACY_IPC_FORMAT",
            messageParameters={},
        )


def require_minimum_numpy_version() -> None:
    """Raise ImportError if minimum version of NumPy is not installed"""
    minimum_numpy_version = "1.21"

    try:
        import numpy

        have_numpy = True
    except ImportError as error:
        have_numpy = False
        raised_error = error
    if not have_numpy:
        raise PySparkImportError(
            errorClass="PACKAGE_NOT_INSTALLED",
            messageParameters={
                "package_name": "NumPy",
                "minimum_version": str(minimum_numpy_version),
            },
        ) from raised_error
    if LooseVersion(numpy.__version__) < LooseVersion(minimum_numpy_version):
        raise PySparkImportError(
            errorClass="UNSUPPORTED_PACKAGE_VERSION",
            messageParameters={
                "package_name": "NumPy",
                "minimum_version": str(minimum_numpy_version),
                "current_version": str(numpy.__version__),
            },
        )
