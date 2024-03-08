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
import numpy as np
import pyspark.pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class ValueCountsTestsMixin:
    def test_value_counts(self):
        pdf = pd.DataFrame(
            {"A": [np.nan, 2, 2, 3, 3, 3], "B": [1, 1, 2, 3, 3, np.nan]}, columns=["A", "B"]
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            psdf.groupby("A")["B"].value_counts().sort_index(),
            pdf.groupby("A")["B"].value_counts().sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")["B"].value_counts(dropna=False).sort_index(),
            pdf.groupby("A")["B"].value_counts(dropna=False).sort_index(),
            almost=True,
        )
        self.assert_eq(
            psdf.groupby("A", dropna=False)["B"].value_counts(dropna=False).sort_index(),
            pdf.groupby("A", dropna=False)["B"].value_counts(dropna=False).sort_index(),
            # Returns are the same considering values and types,
            # disable check_exact to pass the assert_eq
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby("A")["B"].value_counts(sort=True, ascending=False).sort_index(),
            pdf.groupby("A")["B"].value_counts(sort=True, ascending=False).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")["B"]
            .value_counts(sort=True, ascending=False, dropna=False)
            .sort_index(),
            pdf.groupby("A")["B"]
            .value_counts(sort=True, ascending=False, dropna=False)
            .sort_index(),
            almost=True,
        )
        self.assert_eq(
            psdf.groupby("A")["B"]
            .value_counts(sort=True, ascending=True, dropna=False)
            .sort_index(),
            pdf.groupby("A")["B"]
            .value_counts(sort=True, ascending=True, dropna=False)
            .sort_index(),
            almost=True,
        )
        self.assert_eq(
            psdf.B.rename().groupby(psdf.A).value_counts().sort_index(),
            pdf.B.rename().groupby(pdf.A).value_counts().sort_index(),
        )
        self.assert_eq(
            psdf.B.rename().groupby(psdf.A, dropna=False).value_counts().sort_index(),
            pdf.B.rename().groupby(pdf.A, dropna=False).value_counts().sort_index(),
            # Returns are the same considering values and types,
            # disable check_exact to pass the assert_eq
            check_exact=False,
        )
        self.assert_eq(
            psdf.B.groupby(psdf.A.rename()).value_counts().sort_index(),
            pdf.B.groupby(pdf.A.rename()).value_counts().sort_index(),
        )
        self.assert_eq(
            psdf.B.rename().groupby(psdf.A.rename()).value_counts().sort_index(),
            pdf.B.rename().groupby(pdf.A.rename()).value_counts().sort_index(),
        )


class ValueCountsTests(
    ValueCountsTestsMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_value_counts import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
