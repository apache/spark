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
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class IndexesMapMixin:
    def test_map(self):
        pidx = pd.Index([1, 2, 3])
        psidx = ps.from_pandas(pidx)

        # Apply dict
        self.assert_eq(
            pidx.map({1: "one", 2: "two", 3: "three"}),
            psidx.map({1: "one", 2: "two", 3: "three"}),
        )
        self.assert_eq(
            pidx.map({1: "one", 2: "two"}),
            psidx.map({1: "one", 2: "two"}),
        )
        self.assert_eq(
            pidx.map({1: "one", 2: "two"}, na_action="ignore"),
            psidx.map({1: "one", 2: "two"}, na_action="ignore"),
        )
        self.assert_eq(
            pidx.map({1: 10, 2: 20}),
            psidx.map({1: 10, 2: 20}),
        )
        self.assert_eq(
            (pidx + 1).map({1: 10, 2: 20}),
            (psidx + 1).map({1: 10, 2: 20}),
        )

        # Apply lambda
        self.assert_eq(
            pidx.map(lambda id: id + 1),
            psidx.map(lambda id: id + 1),
        )
        self.assert_eq(
            pidx.map(lambda id: id + 1.1),
            psidx.map(lambda id: id + 1.1),
        )
        self.assert_eq(
            pidx.map(lambda id: "{id} + 1".format(id=id)),
            psidx.map(lambda id: "{id} + 1".format(id=id)),
        )
        self.assert_eq(
            (pidx + 1).map(lambda id: "{id} + 1".format(id=id)),
            (psidx + 1).map(lambda id: "{id} + 1".format(id=id)),
        )

        # Apply series
        pser = pd.Series(["one", "two", "three"], index=[1, 2, 3])
        self.assert_eq(
            pidx.map(pser),
            psidx.map(pser),
        )
        pser = pd.Series(["one", "two", "three"])
        self.assert_eq(
            pidx.map(pser),
            psidx.map(pser),
        )
        self.assert_eq(
            pidx.map(pser, na_action="ignore"),
            psidx.map(pser, na_action="ignore"),
        )
        pser = pd.Series([1, 2, 3])
        self.assert_eq(
            pidx.map(pser),
            psidx.map(pser),
        )
        self.assert_eq(
            (pidx + 1).map(pser),
            (psidx + 1).map(pser),
        )

        self.assertRaises(
            TypeError,
            lambda: psidx.map({1: 1, 2: 2.0, 3: "three"}),
        )


class IndexesMapTests(
    IndexesMapMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_map import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
