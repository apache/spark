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
import inspect
import os
import unittest

from pyspark.sql.tests.test_udf_profiler import (
    UDFProfiler2TestsMixin,
    _do_computation,
)
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.utils import have_flameprof


class UDFProfilerParityTests(UDFProfiler2TestsMixin, ReusedConnectTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.spark._profiler_collector._value = None


class UDFProfilerWithoutPlanCacheParityTests(UDFProfilerParityTests):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.conf.set("spark.connect.session.planCache.enabled", False)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.connect.session.planCache.enabled")
        finally:
            super().tearDownClass()

    def test_perf_profiler_udf_multiple_actions(self):
        def action(df):
            df.collect()
            df.show()

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            _do_computation(self.spark, action=action)

        # Without the plan cache, UDF ID will be different for each action
        self.assertEqual(6, len(self.profile_results), str(list(self.profile_results)))

        for id in self.profile_results:
            with self.trap_stdout() as io:
                self.spark.profile.show(id, type="perf")

            self.assertIn(f"Profile of UDF<id={id}>", io.getvalue())
            self.assertRegex(
                io.getvalue(), f"10.*{os.path.basename(inspect.getfile(_do_computation))}"
            )

            if have_flameprof:
                self.assertIn("svg", self.spark.profile.render(id))


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_parity_udf_profiler import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
