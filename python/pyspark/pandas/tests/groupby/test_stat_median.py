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
from pyspark.loose_version import LooseVersion
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.pandas.tests.groupby.test_stat import GroupbyStatTestingFuncMixin, using_pandas3


class GroupbyStatMedianMixin(GroupbyStatTestingFuncMixin):
    @property
    def pdf(self):
        return pd.DataFrame(
            {
                "A": [1, 2, 1, 2],
                "B": [3.1, 4.1, 4.1, 3.1],
                "C": ["a", "b", "b", "a"],
                "D": [True, False, False, True],
            }
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_median(self):
        psdf = ps.DataFrame(
            {
                "a": [1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0],
                "b": [2.0, 3.0, 1.0, 4.0, 6.0, 9.0, 8.0, 10.0, 7.0, 5.0],
                "c": [3.0, 5.0, 2.0, 5.0, 1.0, 2.0, 6.0, 4.0, 3.0, 6.0],
            },
            columns=["a", "b", "c"],
            index=[7, 2, 4, 1, 3, 4, 9, 10, 5, 6],
        )
        # DataFrame
        expected_result = ps.DataFrame(
            {"b": [2.0, 8.0, 7.0], "c": [3.0, 2.0, 4.0]}, index=pd.Index([1.0, 2.0, 3.0], name="a")
        )
        self.assert_eq(expected_result, psdf.groupby("a").median().sort_index())
        # Series
        expected_result = ps.Series(
            [2.0, 8.0, 7.0], name="b", index=pd.Index([1.0, 2.0, 3.0], name="a")
        )
        self.assert_eq(expected_result, psdf.groupby("a")["b"].median().sort_index())

        with self.assertRaisesRegex(TypeError, "accuracy must be an integer; however"):
            psdf.groupby("a").median(accuracy="a")

        if LooseVersion(pd.__version__) >= "3.0.0":
            # pandas < 3 raises an error when numeric_only is False or None
            self._test_stat_func(
                lambda groupby_obj: groupby_obj.median(numeric_only=None),
                expected_error=ValueError if using_pandas3 else None,
            )


class GroupbyStatMedianTests(
    GroupbyStatMedianMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
