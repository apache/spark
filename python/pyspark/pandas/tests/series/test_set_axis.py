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

import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase


# This file contains test cases for 'Series.set_axis'
# https://issues.apache.org/jira/browse/SPARK-56375
class SeriesSetAxisMixin:
    def test_set_axis_index(self):
        pser = pd.Series([1, 2, 3])
        psser = ps.from_pandas(pser)

        # List labels
        self.assert_eq(
            pser.set_axis(["a", "b", "c"]),
            psser.set_axis(["a", "b", "c"]),
        )

        # axis="index" string
        self.assert_eq(
            pser.set_axis(["x", "y", "z"], axis="index"),
            psser.set_axis(["x", "y", "z"], axis="index"),
        )

        # pd.Index
        self.assert_eq(
            pser.set_axis(pd.Index([10, 20, 30])),
            psser.set_axis(pd.Index([10, 20, 30])),
        )

    def test_set_axis_errors(self):
        psser = ps.Series([1, 2, 3])

        # length mismatch
        with self.assertRaises(ValueError):
            psser.set_axis(["a", "b"])  # only 2 labels for 3 elements

        # invalid axis (Series only supports 0/'index')
        with self.assertRaises(ValueError):
            psser.set_axis(["a", "b", "c"], axis=1)

    def test_set_axis_named(self):
        pser = pd.Series([10, 20, 30], name="myval")
        psser = ps.from_pandas(pser)

        self.assert_eq(
            pser.set_axis(["x", "y", "z"]),
            psser.set_axis(["x", "y", "z"]),
        )


class SeriesSetAxisTests(SeriesSetAxisMixin, PandasOnSparkTestCase):
    pass


if __name__ == "__main__":
    import unittest

    unittest.main()
