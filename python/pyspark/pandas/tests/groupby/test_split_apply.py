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

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class GroupbySplitApplyTestingFuncMixin:
    def _test_split_apply_func(self, funcs):
        # TODO(SPARK-45228): Enabling string type columns for `test_split_apply_combine_on_series`
        #  when Pandas regression is fixed
        # There is a regression in Pandas 2.1.0,
        # so we should manually cast to float until the regression is fixed.
        # See https://github.com/pandas-dev/pandas/issues/55194.
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 6, 4, 4, 6, 4, 3, 7],
                "b": [4, 2, 7, 3, 3, 1, 1, 1, 2],
                "c": [4, 2, 7, 3, None, 1, 1, 1, 2],
                # "d": list("abcdefght"),
            },
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )
        psdf = ps.from_pandas(pdf)

        funcs = [
            (
                check_exact,
                almost,
                f,
            )
            for (check_exact, almost), fs in funcs
            for f in fs
        ]

        for as_index in [True, False]:
            if as_index:

                def sort(df):
                    return df.sort_index()

            else:

                def sort(df):
                    return df.sort_values(list(df.columns)).reset_index(drop=True)

            for check_exact, almost, func in funcs:
                for kkey, pkey in [("b", "b"), (psdf.b, pdf.b)]:
                    with self.subTest(as_index=as_index, func=func, key=pkey):
                        if as_index is True or func != "std":
                            self.assert_eq(
                                sort(getattr(psdf.groupby(kkey, as_index=as_index).a, func)()),
                                sort(getattr(pdf.groupby(pkey, as_index=as_index).a, func)()),
                                check_exact=check_exact,
                                almost=almost,
                            )
                            self.assert_eq(
                                sort(getattr(psdf.groupby(kkey, as_index=as_index), func)()),
                                sort(getattr(pdf.groupby(pkey, as_index=as_index), func)()),
                                check_exact=check_exact,
                                almost=almost,
                            )
                        else:
                            # seems like a pandas' bug for as_index=False and func == "std"?
                            self.assert_eq(
                                sort(getattr(psdf.groupby(kkey, as_index=as_index).a, func)()),
                                sort(pdf.groupby(pkey, as_index=True).a.std().reset_index()),
                                check_exact=check_exact,
                                almost=almost,
                            )
                            self.assert_eq(
                                sort(getattr(psdf.groupby(kkey, as_index=as_index), func)()),
                                sort(pdf.groupby(pkey, as_index=True).std().reset_index()),
                                check_exact=check_exact,
                                almost=almost,
                            )

                for kkey, pkey in [(psdf.b + 1, pdf.b + 1), (psdf.copy().b, pdf.copy().b)]:
                    with self.subTest(as_index=as_index, func=func, key=pkey):
                        self.assert_eq(
                            sort(getattr(psdf.groupby(kkey, as_index=as_index).a, func)()),
                            sort(getattr(pdf.groupby(pkey, as_index=as_index).a, func)()),
                            check_exact=check_exact,
                            almost=almost,
                        )
                        self.assert_eq(
                            sort(getattr(psdf.groupby(kkey, as_index=as_index), func)()),
                            sort(getattr(pdf.groupby(pkey, as_index=as_index), func)()),
                            check_exact=check_exact,
                            almost=almost,
                        )

            for check_exact, almost, func in funcs:
                for i in [0, 4, 7]:
                    with self.subTest(as_index=as_index, func=func, i=i):
                        self.assert_eq(
                            sort(getattr(psdf.groupby(psdf.b > i, as_index=as_index).a, func)()),
                            sort(getattr(pdf.groupby(pdf.b > i, as_index=as_index).a, func)()),
                            check_exact=check_exact,
                            almost=almost,
                        )
                        self.assert_eq(
                            sort(getattr(psdf.groupby(psdf.b > i, as_index=as_index), func)()),
                            sort(getattr(pdf.groupby(pdf.b > i, as_index=as_index), func)()),
                            check_exact=check_exact,
                            almost=almost,
                        )

        for check_exact, almost, func in funcs:
            for kkey, pkey in [
                (psdf.b, pdf.b),
                (psdf.b + 1, pdf.b + 1),
                (psdf.copy().b, pdf.copy().b),
                (psdf.b.rename(), pdf.b.rename()),
            ]:
                with self.subTest(func=func, key=pkey):
                    self.assert_eq(
                        getattr(psdf.a.groupby(kkey), func)().sort_index(),
                        getattr(pdf.a.groupby(pkey), func)().sort_index(),
                        check_exact=check_exact,
                        almost=almost,
                    )
                    self.assert_eq(
                        getattr((psdf.a + 1).groupby(kkey), func)().sort_index(),
                        getattr((pdf.a + 1).groupby(pkey), func)().sort_index(),
                        check_exact=check_exact,
                        almost=almost,
                    )
                    self.assert_eq(
                        getattr((psdf.b + 1).groupby(kkey), func)().sort_index(),
                        getattr((pdf.b + 1).groupby(pkey), func)().sort_index(),
                        check_exact=check_exact,
                        almost=almost,
                    )
                    self.assert_eq(
                        getattr(psdf.a.rename().groupby(kkey), func)().sort_index(),
                        getattr(pdf.a.rename().groupby(pkey), func)().sort_index(),
                        check_exact=check_exact,
                        almost=almost,
                    )


class GroupbySplitApplyMixin(GroupbySplitApplyTestingFuncMixin):
    def test_split_apply_combine_on_series(self):
        funcs = [
            ((True, False), ["sum"]),
            ((True, True), ["mean"]),
        ]
        self._test_split_apply_func(funcs)


class GroupbySplitApplyTests(
    GroupbySplitApplyMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_split_apply import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
