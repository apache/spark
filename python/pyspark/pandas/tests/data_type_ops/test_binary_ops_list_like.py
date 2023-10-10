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
import numpy as np
from pandas.api.types import CategoricalDtype

from pyspark import pandas as ps
from pyspark.pandas.tests.data_type_ops.testing_utils import OpsTestBase


class ListLikeOpsTestMixin:
    @property
    def pser(self):
        return pd.Series([1, 2, 3, 4, 5, 6], name="x")

    @property
    def psser(self):
        return ps.from_pandas(self.pser)

    @property
    def other(self):
        return [np.nan, 1, 3, 4, np.nan, 5]

    def test_le(self):
        pser, psser = self.pser, self.psser
        self.assert_eq(psser <= self.other, pser <= self.other)

    def test_lt(self):
        pser, psser = self.pser, self.psser
        self.assert_eq(psser < self.other, pser < self.other)

    def test_ge(self):
        pser, psser = self.pser, self.psser
        self.assert_eq(psser >= self.other, pser >= self.other)

    def test_gt(self):
        pser, psser = self.pser, self.psser
        self.assert_eq(psser > self.other, pser > self.other)

    def test_ne(self):
        pser, psser = self.pser, self.psser
        self.assert_eq(psser != self.other, pser != self.other)


class ListListOpsTests(ListLikeOpsTestMixin, OpsTestBase):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.data_type_ops.test_binary_ops_list_like import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
