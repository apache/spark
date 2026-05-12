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
from pyspark.loose_version import LooseVersion
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class CovMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {
                "A": [1, 1, 2, 2, 2, 3],
                "B": [-1, 2, 3, 5, 6, 0],
                "C": [4, 6, 5, 1, 3, 0],
            },
            columns=["A", "B", "C"],
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_cov(self):
        for c in ["A", "B", "C"]:
            self.assert_eq(
                self.pdf.groupby(c).cov().sort_index(),
                self.psdf.groupby(c).cov().sort_index(),
                almost=True,
            )

    def test_ddof(self):
        # Use a dataset with enough rows per group to keep N - ddof > 0,
        # since pandas and Spark diverge on inf/NaN handling when the divisor is non-positive.
        pdf = pd.DataFrame(
            {
                "A": [1, 1, 1, 1, 2, 2, 2, 2],
                "B": [1, 2, 3, 4, 5, 6, 7, 8],
                "C": [4, 6, 5, 1, 3, 0, 9, 2],
            },
            columns=["A", "B", "C"],
        )
        psdf = ps.from_pandas(pdf)
        for ddof in [0, 1, 2]:
            self.assert_eq(
                pdf.groupby("A").cov(ddof=ddof).sort_index(),
                psdf.groupby("A").cov(ddof=ddof).sort_index(),
                almost=True,
            )

    def test_min_periods(self):
        pdf = pd.DataFrame(
            {
                "A": [1, 1, 1, 1, 2, 2],
                "B": [1.0, 2.0, np.nan, 4.0, 5.0, 6.0],
                "C": [4.0, 6.0, 5.0, 7.0, 1.0, 3.0],
            },
            columns=["A", "B", "C"],
        )
        psdf = ps.from_pandas(pdf)
        for m in [1, 2, 3, 4]:
            self.assert_eq(
                pdf.groupby("A").cov(min_periods=m).sort_index(),
                psdf.groupby("A").cov(min_periods=m).sort_index(),
                almost=True,
            )

    def test_numeric_only(self):
        pdf = pd.DataFrame(
            {
                "A": [1, 1, 2, 2, 2],
                "B": [1, 2, 3, 4, 5],
                "C": [4, 6, 5, 1, 3],
                "D": ["x", "y", "z", "w", "v"],
            },
            columns=["A", "B", "C", "D"],
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.groupby("A").cov(numeric_only=True).sort_index(),
            psdf.groupby("A").cov(numeric_only=True).sort_index(),
            almost=True,
        )

    def test_invalid_args(self):
        with self.assertRaisesRegex(TypeError, "ddof must be integer"):
            self.psdf.groupby("A").cov(ddof="1")

        if LooseVersion(pd.__version__) >= "3.0.0":
            with self.assertRaisesRegex(ValueError, "numeric_only accepts only Boolean values"):
                self.psdf.groupby("A").cov(numeric_only="True")


class CovTests(
    CovMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
