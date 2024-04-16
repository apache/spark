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
from pyspark.pandas.utils import name_like_string
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class GetDummiesMultiIndexMixin:
    def test_get_dummies_multiindex_columns(self):
        pdf = pd.DataFrame(
            {
                ("x", "a", "1"): [1, 2, 3, 4, 4, 3, 2, 1],
                ("x", "b", "2"): list("abcdabcd"),
                ("y", "c", "3"): list("abcdabcd"),
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            ps.get_dummies(psdf),
            pd.get_dummies(pdf, dtype=np.int8).rename(columns=name_like_string),
        )
        self.assert_eq(
            ps.get_dummies(psdf, columns=[("y", "c", "3"), ("x", "a", "1")]),
            pd.get_dummies(pdf, columns=[("y", "c", "3"), ("x", "a", "1")], dtype=np.int8).rename(
                columns=name_like_string
            ),
        )
        self.assert_eq(
            ps.get_dummies(psdf, columns=["x"]),
            pd.get_dummies(pdf, columns=["x"], dtype=np.int8).rename(columns=name_like_string),
        )
        self.assert_eq(
            ps.get_dummies(psdf, columns=("x", "a")),
            pd.get_dummies(pdf, columns=("x", "a"), dtype=np.int8).rename(columns=name_like_string),
        )

        self.assertRaises(KeyError, lambda: ps.get_dummies(psdf, columns=["z"]))
        self.assertRaises(KeyError, lambda: ps.get_dummies(psdf, columns=("x", "c")))
        self.assertRaises(ValueError, lambda: ps.get_dummies(psdf, columns=[("x",), "c"]))
        self.assertRaises(TypeError, lambda: ps.get_dummies(psdf, columns="x"))

        # non-string names
        pdf = pd.DataFrame(
            {
                ("x", 1, "a"): [1, 2, 3, 4, 4, 3, 2, 1],
                ("x", 2, "b"): list("abcdabcd"),
                ("y", 3, "c"): list("abcdabcd"),
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            ps.get_dummies(psdf),
            pd.get_dummies(pdf, dtype=np.int8).rename(columns=name_like_string),
        )
        self.assert_eq(
            ps.get_dummies(psdf, columns=[("y", 3, "c"), ("x", 1, "a")]),
            pd.get_dummies(pdf, columns=[("y", 3, "c"), ("x", 1, "a")], dtype=np.int8).rename(
                columns=name_like_string
            ),
        )
        self.assert_eq(
            ps.get_dummies(psdf, columns=["x"]),
            pd.get_dummies(pdf, columns=["x"], dtype=np.int8).rename(columns=name_like_string),
        )
        self.assert_eq(
            ps.get_dummies(psdf, columns=("x", 1)),
            pd.get_dummies(pdf, columns=("x", 1), dtype=np.int8).rename(columns=name_like_string),
        )


class GetDummiesMultiIndexTests(
    GetDummiesMultiIndexMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.reshape.test_get_dummies_multiindex import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
