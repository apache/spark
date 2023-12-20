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

import pyspark.pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class FrameInterpolateErrorMixin:
    def test_interpolate_error(self):
        psdf = ps.range(10)

        with self.assertRaisesRegex(
            NotImplementedError, "interpolate currently works only for method='linear'"
        ):
            psdf.interpolate(method="quadratic")

        with self.assertRaisesRegex(
            NotImplementedError, "interpolate currently works only for method='linear'"
        ):
            psdf.id.interpolate(method="quadratic")

        with self.assertRaisesRegex(ValueError, "limit must be > 0"):
            psdf.interpolate(limit=0)

        with self.assertRaisesRegex(ValueError, "limit must be > 0"):
            psdf.id.interpolate(limit=0)

        with self.assertRaisesRegex(ValueError, "invalid limit_direction"):
            psdf.interpolate(limit_direction="jump")

        with self.assertRaisesRegex(ValueError, "invalid limit_direction"):
            psdf.id.interpolate(limit_direction="jump")

        with self.assertRaisesRegex(ValueError, "invalid limit_area"):
            psdf.interpolate(limit_area="jump")

        with self.assertRaisesRegex(ValueError, "invalid limit_area"):
            psdf.id.interpolate(limit_area="jump")

        with self.assertRaisesRegex(
            TypeError, "Cannot interpolate with all object-dtype columns in the DataFrame."
        ):
            ps.DataFrame({"A": ["a", "b", "c"], "B": ["a", "b", "c"]}).interpolate()


class FrameInterpolateErrorTests(
    FrameInterpolateErrorMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.frame.test_interpolate_error import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
