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


class GetDummiesObjectMixin:
    def test_get_dummies_object(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 4, 3, 2, 1],
                # 'a': pd.Categorical([1, 2, 3, 4, 4, 3, 2, 1]),
                "b": list("abcdabcd"),
                # 'c': pd.Categorical(list('abcdabcd')),
                "c": list("abcdabcd"),
            }
        )
        psdf = ps.from_pandas(pdf)

        # Explicitly exclude object columns
        self.assert_eq(
            ps.get_dummies(psdf, columns=["a", "c"]),
            pd.get_dummies(pdf, columns=["a", "c"], dtype=np.int8),
        )

        self.assert_eq(ps.get_dummies(psdf), pd.get_dummies(pdf, dtype=np.int8))
        self.assert_eq(ps.get_dummies(psdf.b), pd.get_dummies(pdf.b, dtype=np.int8))
        self.assert_eq(
            ps.get_dummies(psdf, columns=["b"]), pd.get_dummies(pdf, columns=["b"], dtype=np.int8)
        )

        self.assertRaises(KeyError, lambda: ps.get_dummies(psdf, columns=("a", "c")))
        self.assertRaises(TypeError, lambda: ps.get_dummies(psdf, columns="b"))

        # non-string names
        pdf = pd.DataFrame(
            {10: [1, 2, 3, 4, 4, 3, 2, 1], 20: list("abcdabcd"), 30: list("abcdabcd")}
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            ps.get_dummies(psdf, columns=[10, 30]),
            pd.get_dummies(pdf, columns=[10, 30], dtype=np.int8),
        )

        self.assertRaises(TypeError, lambda: ps.get_dummies(psdf, columns=10))


class GetDummiesObjectTests(
    GetDummiesObjectMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.reshape.test_get_dummies_object import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
