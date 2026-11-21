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
"""
Re-runs the Arrow-optimized Python UDF suites with transpilation enabled.

The Arrow counterpart of ``pyspark.sql.tests.test_udf_transpile_parity``. Both conversion
regimes are run because they are different references: under the legacy pandas conversion
transpilation must be refused outright, and this is the suite that breaks if that gate is
missing. Transpilation is non-Connect only, hence the ``is_remote_only()`` guards.
"""

import unittest

from pyspark.sql.tests.arrow.test_arrow_python_udf import (
    ArrowPythonUDFLegacyTestsMixin,
    ArrowPythonUDFNonLegacyTestsMixin,
)
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.util import is_remote_only

# Transpilation is gated on both of these, at UDF construction time
# (python/pyspark/sql/udf.py) and again in the Catalyst optimizer (ConvertToCatalyst).
# spark.conf.set takes strings, so "true" rather than Python True.
_TRANSPILE_CONF = {
    "spark.sql.experimental.optimizer.transpilePyUDFs": "true",
    "spark.sql.ansi.enabled": "true",
}

_NON_CONNECT_ONLY = "UDF transpilation is only supported in regular (non-Connect) Spark."


def _enable_transpilation(cls):
    for key, value in _TRANSPILE_CONF.items():
        cls.spark.conf.set(key, value)


def _disable_transpilation(cls):
    for key in _TRANSPILE_CONF:
        cls.spark.conf.unset(key)


@unittest.skipIf(is_remote_only(), _NON_CONNECT_ONLY)
@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,  # type: ignore[arg-type]
)
class TranspiledArrowPythonUDFNonLegacyParityTests(
    ArrowPythonUDFNonLegacyTestsMixin, ReusedSQLTestCase
):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "true")
        _enable_transpilation(cls)

    @classmethod
    def tearDownClass(cls):
        try:
            _disable_transpilation(cls)
            cls.spark.conf.unset("spark.sql.execution.pythonUDF.arrow.enabled")
        finally:
            super().tearDownClass()


@unittest.skipIf(is_remote_only(), _NON_CONNECT_ONLY)
@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,  # type: ignore[arg-type]
)
class TranspiledArrowPythonUDFLegacyParityTests(ArrowPythonUDFLegacyTestsMixin, ReusedSQLTestCase):
    """The legacy pandas conversion regime, with transpilation requested.

    Transpilation is expected to be refused outright here: the pandas round trip changes
    what the Python function receives, so a rewrite modelled on the non-legacy Arrow
    semantics would answer differently. If the gate for that is missing, this is the suite
    that surfaces it -- as a wrong answer somewhere in the shared UDF mixin rather than as
    an obvious failure, which is why it is worth running explicitly.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDF.arrow.concurrency.level", "4")
        cls.spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "true")
        _enable_transpilation(cls)

    @classmethod
    def tearDownClass(cls):
        try:
            _disable_transpilation(cls)
            cls.spark.conf.unset("spark.sql.execution.pythonUDF.arrow.concurrency.level")
            cls.spark.conf.unset("spark.sql.execution.pythonUDF.arrow.enabled")
        finally:
            super().tearDownClass()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
