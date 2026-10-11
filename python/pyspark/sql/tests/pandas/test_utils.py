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

import os
import sys
import types
import unittest
import warnings
from typing import Optional
from unittest import mock

from pyspark.errors import PySparkImportError, PySparkRuntimeError
from pyspark.sql.pandas.utils import (
    require_minimum_numpy_version,
    require_minimum_pandas_version,
    require_minimum_pyarrow_version,
)
from pyspark.testing.utils import PySparkErrorTestUtils


def _fake_module(name: str, version: Optional[str]) -> types.ModuleType:
    # A stand-in module so that the tests do not depend on the installed packages.
    module = types.ModuleType(name)
    if version is not None:
        setattr(module, "__version__", version)
    return module


class PandasVersionCheckTests(unittest.TestCase, PySparkErrorTestUtils):
    def test_pandas_not_installed(self):
        with mock.patch.dict(sys.modules, {"pandas": None}):
            with self.assertRaises(PySparkImportError) as pe:
                require_minimum_pandas_version()

        self.check_error(
            exception=pe.exception,
            errorClass="PACKAGE_NOT_INSTALLED",
            messageParameters={"package_name": "Pandas", "minimum_version": "2.2.0"},
        )
        self.assertIsInstance(pe.exception.__cause__, ImportError)

    def test_pandas_without_version(self):
        # e.g. pandas was removed but pandas-stubs left a 'pandas' package behind.
        with mock.patch.dict(sys.modules, {"pandas": _fake_module("pandas", None)}):
            with self.assertRaises(PySparkImportError) as pe:
                require_minimum_pandas_version()

        self.check_error(
            exception=pe.exception,
            errorClass="PACKAGE_NOT_INSTALLED",
            messageParameters={"package_name": "Pandas", "minimum_version": "2.2.0"},
        )
        self.assertIsNone(pe.exception.__cause__)

    def test_pandas_too_old(self):
        with mock.patch.dict(sys.modules, {"pandas": _fake_module("pandas", "2.1.4")}):
            with self.assertRaises(PySparkImportError) as pe:
                require_minimum_pandas_version()

        self.check_error(
            exception=pe.exception,
            errorClass="UNSUPPORTED_PACKAGE_VERSION",
            messageParameters={
                "package_name": "Pandas",
                "minimum_version": "2.2.0",
                "current_version": "2.1.4",
            },
        )

    def test_pandas_supported(self):
        for version in ["2.2.0", "2.3.3"]:
            with self.subTest(version=version):
                with mock.patch.dict(sys.modules, {"pandas": _fake_module("pandas", version)}):
                    with warnings.catch_warnings():
                        warnings.simplefilter("error")
                        require_minimum_pandas_version()

    def test_pandas_3_warns(self):
        for version in ["3.0.0", "3.1.0"]:
            with self.subTest(version=version):
                with mock.patch.dict(sys.modules, {"pandas": _fake_module("pandas", version)}):
                    with self.assertWarnsRegex(FutureWarning, "pandas >= 3.0.0"):
                        require_minimum_pandas_version()


class PyArrowVersionCheckTests(unittest.TestCase, PySparkErrorTestUtils):
    def setUp(self):
        # Make sure the legacy IPC format check is not triggered by the environment.
        env_patcher = mock.patch.dict(os.environ)
        env_patcher.start()
        self.addCleanup(env_patcher.stop)
        os.environ.pop("ARROW_PRE_0_15_IPC_FORMAT", None)

    def test_pyarrow_not_installed(self):
        with mock.patch.dict(sys.modules, {"pyarrow": None}):
            with self.assertRaises(PySparkImportError) as pe:
                require_minimum_pyarrow_version()

        self.check_error(
            exception=pe.exception,
            errorClass="PACKAGE_NOT_INSTALLED",
            messageParameters={"package_name": "PyArrow", "minimum_version": "18.0.0"},
        )
        self.assertIsInstance(pe.exception.__cause__, ImportError)

    def test_pyarrow_too_old(self):
        with mock.patch.dict(sys.modules, {"pyarrow": _fake_module("pyarrow", "17.0.0")}):
            with self.assertRaises(PySparkImportError) as pe:
                require_minimum_pyarrow_version()

        self.check_error(
            exception=pe.exception,
            errorClass="UNSUPPORTED_PACKAGE_VERSION",
            messageParameters={
                "package_name": "PyArrow",
                "minimum_version": "18.0.0",
                "current_version": "17.0.0",
            },
        )

    def test_pyarrow_supported(self):
        for version in ["18.0.0", "25.0.1"]:
            with self.subTest(version=version):
                with mock.patch.dict(sys.modules, {"pyarrow": _fake_module("pyarrow", version)}):
                    require_minimum_pyarrow_version()

    def test_pyarrow_legacy_ipc_format(self):
        with mock.patch.dict(sys.modules, {"pyarrow": _fake_module("pyarrow", "18.0.0")}):
            with mock.patch.dict(os.environ, {"ARROW_PRE_0_15_IPC_FORMAT": "1"}):
                with self.assertRaises(PySparkRuntimeError) as pe:
                    require_minimum_pyarrow_version()

        self.check_error(
            exception=pe.exception,
            errorClass="ARROW_LEGACY_IPC_FORMAT",
            messageParameters={},
        )

    def test_pyarrow_legacy_ipc_format_disabled(self):
        with mock.patch.dict(sys.modules, {"pyarrow": _fake_module("pyarrow", "18.0.0")}):
            with mock.patch.dict(os.environ, {"ARROW_PRE_0_15_IPC_FORMAT": "0"}):
                require_minimum_pyarrow_version()


class NumPyVersionCheckTests(unittest.TestCase, PySparkErrorTestUtils):
    def test_numpy_not_installed(self):
        with mock.patch.dict(sys.modules, {"numpy": None}):
            with self.assertRaises(PySparkImportError) as pe:
                require_minimum_numpy_version()

        self.check_error(
            exception=pe.exception,
            errorClass="PACKAGE_NOT_INSTALLED",
            messageParameters={"package_name": "NumPy", "minimum_version": "1.23.2"},
        )
        self.assertIsInstance(pe.exception.__cause__, ImportError)

    def test_numpy_too_old(self):
        with mock.patch.dict(sys.modules, {"numpy": _fake_module("numpy", "1.23.1")}):
            with self.assertRaises(PySparkImportError) as pe:
                require_minimum_numpy_version()

        self.check_error(
            exception=pe.exception,
            errorClass="UNSUPPORTED_PACKAGE_VERSION",
            messageParameters={
                "package_name": "NumPy",
                "minimum_version": "1.23.2",
                "current_version": "1.23.1",
            },
        )

    def test_numpy_supported(self):
        for version in ["1.23.2", "2.5.4"]:
            with self.subTest(version=version):
                with mock.patch.dict(sys.modules, {"numpy": _fake_module("numpy", version)}):
                    require_minimum_numpy_version()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
