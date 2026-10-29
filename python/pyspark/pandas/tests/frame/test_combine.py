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

import pyspark.pandas as ps
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


def take_smaller(s1, s2):
    return pd.Series(np.where(s1 < s2, s1, s2), index=s1.index, name=s1.name)


class CombineMixin:
    @property
    def pdf(self):
        return pd.DataFrame({"a": [1, 2, 3, 4], "b": [4, 5, 6, 7]})

    @property
    def other(self):
        return pd.DataFrame({"a": [4, 3, 2, 1], "b": [7, 6, 5, 4]})

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_disabled(self):
        with self.assertRaises(PandasNotImplementedError):
            self.psdf.combine(self.other, take_smaller)

    def test_fallback(self):
        ps.set_option("compute.pandas_fallback", True)

        self.assert_eq(
            self.pdf.combine(self.other, take_smaller),
            self.psdf.combine(self.other, take_smaller),
        )
        self.assert_eq(
            self.pdf.combine(self.other, take_smaller, overwrite=False),
            self.psdf.combine(self.other, take_smaller, overwrite=False),
        )

        ps.reset_option("compute.pandas_fallback")


class CombineTests(
    CombineMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
