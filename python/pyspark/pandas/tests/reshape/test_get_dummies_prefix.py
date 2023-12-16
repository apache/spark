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

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class GetDummiesPrefixMixin:
    def test_get_dummies_prefix(self):
        pdf = pd.DataFrame({"A": ["a", "b", "a"], "B": ["b", "a", "c"], "D": [0, 0, 1]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            ps.get_dummies(psdf, prefix=["foo", "bar"]),
            pd.get_dummies(pdf, prefix=["foo", "bar"], dtype=np.int8),
        )

        self.assert_eq(
            ps.get_dummies(psdf, prefix=["foo"], columns=["B"]),
            pd.get_dummies(pdf, prefix=["foo"], columns=["B"], dtype=np.int8),
        )

        self.assert_eq(
            ps.get_dummies(psdf, prefix={"A": "foo", "B": "bar"}),
            pd.get_dummies(pdf, prefix={"A": "foo", "B": "bar"}, dtype=np.int8),
        )

        self.assert_eq(
            ps.get_dummies(psdf, prefix={"B": "foo", "A": "bar"}),
            pd.get_dummies(pdf, prefix={"B": "foo", "A": "bar"}, dtype=np.int8),
        )

        self.assert_eq(
            ps.get_dummies(psdf, prefix={"A": "foo", "B": "bar"}, columns=["A", "B"]),
            pd.get_dummies(pdf, prefix={"A": "foo", "B": "bar"}, columns=["A", "B"], dtype=np.int8),
        )

        with self.assertRaisesRegex(NotImplementedError, "string types"):
            ps.get_dummies(psdf, prefix="foo")
        with self.assertRaisesRegex(ValueError, "Length of 'prefix' \\(1\\) .* \\(2\\)"):
            ps.get_dummies(psdf, prefix=["foo"])
        with self.assertRaisesRegex(ValueError, "Length of 'prefix' \\(2\\) .* \\(1\\)"):
            ps.get_dummies(psdf, prefix=["foo", "bar"], columns=["B"])

        pser = pd.Series([1, 1, 1, 2, 2, 1, 3, 4], name="A")
        psser = ps.from_pandas(pser)

        self.assert_eq(
            ps.get_dummies(psser, prefix="foo"), pd.get_dummies(pser, prefix="foo", dtype=np.int8)
        )

        # columns are ignored.
        self.assert_eq(
            ps.get_dummies(psser, prefix=["foo"], columns=["B"]),
            pd.get_dummies(pser, prefix=["foo"], columns=["B"], dtype=np.int8),
        )


class GetDummiesPrefixTests(
    GetDummiesPrefixMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.reshape.test_get_dummies_prefix import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
