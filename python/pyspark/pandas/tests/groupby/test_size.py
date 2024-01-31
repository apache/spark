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

import unittest

import pandas as pd
import pyspark.pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class SizeTestsMixin:
    def test_size(self):
        pdf = pd.DataFrame({"A": [1, 2, 2, 3, 3, 3], "B": [1, 1, 2, 3, 3, 3]})
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.groupby("A").size().sort_index(), pdf.groupby("A").size().sort_index())
        self.assert_eq(
            psdf.groupby("A")["B"].size().sort_index(), pdf.groupby("A")["B"].size().sort_index()
        )
        self.assert_eq(
            psdf.groupby("A")[["B"]].size().sort_index(),
            pdf.groupby("A")[["B"]].size().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["A", "B"]).size().sort_index(),
            pdf.groupby(["A", "B"]).size().sort_index(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("Y", "B")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("X", "A")).size().sort_index(),
            pdf.groupby(("X", "A")).size().sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("X", "A"), ("Y", "B")]).size().sort_index(),
            pdf.groupby([("X", "A"), ("Y", "B")]).size().sort_index(),
        )


class SizeTests(
    SizeTestsMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_size import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
