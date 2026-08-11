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
Parity test that re-runs the scalar pandas UDF suite with transpilation enabled.

The pandas counterpart of ``pyspark.sql.tests.test_udf_transpile_parity`` and
``pyspark.sql.tests.arrow.test_arrow_python_udf_transpile_parity``, and the widest-blast-radius
signal in the change: ``ScalarPandasUDFTestsMixin`` is the existing suite for this eval type,
so running all of it with the rewrite active asserts that every one of its UDFs either agrees
with what the interpreted pandas path produced or was declined by the element-wise gate.

Most of that mixin's UDFs use pandas idioms outside the transpiled subset (``.apply``,
``.astype``, ``pd.concat``, string accessors, struct and timestamp handling), so they exercise
the *fallback* rather than the rewrite -- which is the point. The bodies that do get rewritten
are covered directly, with pinned values, in
``pyspark.sql.tests.pandas.test_pandas_udf_transpile``.

Enabling transpilation requires ANSI mode, so an "on" run is unavoidably also an ANSI run. If a
future change makes an inherited test diverge purely because of ANSI semantics, or because the
rewrite bypasses a Python-side effect the test observes, override it here with a documented
``unittest.skip`` rather than editing the inherited test body.

Transpilation is non-Connect only, hence the ``is_remote_only()`` guard.
"""

import os
import time
import unittest

from pyspark.sql.tests.pandas.test_pandas_udf_scalar import ScalarPandasUDFTestsMixin
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


@unittest.skipIf(is_remote_only(), _NON_CONNECT_ONLY)
@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,  # type: ignore[arg-type]
)
class TranspiledScalarPandasUDFParityTests(ScalarPandasUDFTestsMixin, ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        ReusedSQLTestCase.setUpClass()

        # Synchronize the default timezone between Python and Java, as ScalarPandasUDFTests
        # does: several inherited tests compare timestamps rendered on both sides.
        cls.tz_prev = os.environ.get("TZ", None)
        tz = "America/Los_Angeles"
        os.environ["TZ"] = tz
        time.tzset()
        cls.sc.environment["TZ"] = tz
        cls.spark.conf.set("spark.sql.session.timeZone", tz)

        for key, value in _TRANSPILE_CONF.items():
            cls.spark.conf.set(key, value)

    @classmethod
    def tearDownClass(cls):
        try:
            for key in _TRANSPILE_CONF:
                cls.spark.conf.unset(key)
            del os.environ["TZ"]
            if cls.tz_prev is not None:
                os.environ["TZ"] = cls.tz_prev
            time.tzset()
        finally:
            ReusedSQLTestCase.tearDownClass()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
