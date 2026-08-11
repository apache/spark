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
Parity tests that re-run the existing UDF test suites with Python-to-Catalyst
transpilation enabled.

Transpilation is only attempted when both
``spark.sql.experimental.optimizer.transpilePyUDFs`` and
``spark.sql.ansi.enabled`` are true, and it is designed to fall back to
interpreted Python rather than risk semantic drift. These classes re-run the
shared UDF mixins under that configuration so we can confirm that turning on the
experimental feature does not change UDF results compared with the default
(transpilation off) runs covered by the original concrete classes
(``UDFTests``, ``UDFCombinationsTests``, ``UnifiedUDFTests``).

Transpilation is currently only supported in regular (non-Connect) Spark, so
these classes are guarded with ``is_remote_only()`` and are intentionally not
inherited into the Spark Connect parity tests. The companion suites that test
the transpiler directly live in ``test_udf_transpile_unit.py`` and
``test_udf_transpile_hypothesis.py``.

Note on configuration: enabling transpilation requires ANSI mode, so an "on"
run is unavoidably also an ANSI run. All inherited tests currently pass as-is
under this configuration, so no per-test overrides are defined here. If a future
change makes an inherited test diverge purely due to ANSI semantics or because
transpilation bypasses a Python-side effect (rather than a genuine result
change), override it here with a documented ``unittest.skip`` rather than
editing the inherited test body.
"""

import unittest

from pyspark.sql.tests.test_udf import BaseUDFTestsMixin
from pyspark.sql.tests.test_udf_combinations import UDFCombinationsTestsMixin
from pyspark.sql.tests.test_unified_udf import UnifiedUDFTestsMixin
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.util import is_remote_only


# Transpilation is gated on both of these being enabled, both at UDF
# construction time (python/pyspark/sql/udf.py) and again in the Catalyst
# optimizer (the ConvertToCatalyst rule).
# spark.conf.set requires strings, so we use "true" rather than Python True here.
_TRANSPILE_CONF = {
    "spark.sql.experimental.optimizer.transpilePyUDFs": "true",
    "spark.sql.ansi.enabled": "true",
}

_NON_CONNECT_ONLY = "UDF transpilation is only supported in regular (non-Connect) Spark."


def _enable_transpilation(cls):
    for key, value in _TRANSPILE_CONF.items():
        cls.spark.conf.set(key, value)


@unittest.skipIf(is_remote_only(), _NON_CONNECT_ONLY)
class TranspiledUDFParityTests(BaseUDFTestsMixin, ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        ReusedSQLTestCase.setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "false")
        _enable_transpilation(cls)


@unittest.skipIf(is_remote_only(), _NON_CONNECT_ONLY)
class TranspiledUDFCombinationsParityTests(UDFCombinationsTestsMixin, ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        ReusedSQLTestCase.setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "false")
        _enable_transpilation(cls)


@unittest.skipIf(is_remote_only(), _NON_CONNECT_ONLY)
@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,
)
class TranspiledUnifiedUDFParityTests(UnifiedUDFTestsMixin, ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        ReusedSQLTestCase.setUpClass()
        _enable_transpilation(cls)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
