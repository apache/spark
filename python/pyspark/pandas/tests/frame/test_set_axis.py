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


# This file contains test cases for 'DataFrame.set_axis'
# https://issues.apache.org/jira/browse/SPARK-56375
class FrameSetAxisMixin:
    def test_set_axis_index(self):
        pdf = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
        psdf = ps.from_pandas(pdf)

        # axis=0 (index), list labels
        self.assert_eq(
            pdf.set_axis(["x", "y", "z"]),
            psdf.set_axis(["x", "y", "z"]),
        )

        # axis=0 with "index" string
        self.assert_eq(
            pdf.set_axis(["a", "b", "c"], axis="index"),
            psdf.set_axis(["a", "b", "c"], axis="index"),
        )

        # axis=0 with pd.Index
        self.assert_eq(
            pdf.set_axis(pd.Index([10, 20, 30])),
            psdf.set_axis(pd.Index([10, 20, 30])),
        )

    def test_set_axis_columns(self):
        pdf = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
        psdf = ps.from_pandas(pdf)

        # axis=1 (columns), list labels
        self.assert_eq(
            pdf.set_axis(["I", "II"], axis=1),
            psdf.set_axis(["I", "II"], axis=1),
        )

        # axis=1 with "columns" string
        self.assert_eq(
            pdf.set_axis(["col1", "col2"], axis="columns"),
            psdf.set_axis(["col1", "col2"], axis="columns"),
        )

        # axis=1 with pd.Index
        self.assert_eq(
            pdf.set_axis(pd.Index(["X", "Y"]), axis=1),
            psdf.set_axis(pd.Index(["X", "Y"]), axis=1),
        )

    def test_set_axis_errors(self):
        psdf = ps.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})

        # length mismatch on index (axis=0)
        with self.assertRaises(ValueError):
            psdf.set_axis(["x", "y"])  # only 2 labels for 3 rows

        # length mismatch on columns (axis=1)
        with self.assertRaises(ValueError):
            psdf.set_axis(["X"], axis=1)  # only 1 label for 2 columns

        # invalid axis
        with self.assertRaises(ValueError):
            psdf.set_axis(["x", "y", "z"], axis=2)

    def test_set_axis_numeric_index(self):
        pdf = pd.DataFrame({"A": [10, 20, 30], "B": [40, 50, 60]}, index=[0, 1, 2])
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.set_axis([100, 200, 300]),
            psdf.set_axis([100, 200, 300]),
        )


class FrameSetAxisTests(FrameSetAxisMixin, PandasOnSparkTestCase):
    pass


if __name__ == "__main__":
    import unittest

    from pyspark.testing.utils import PySparkTestCase  # noqa

    unittest.main()
