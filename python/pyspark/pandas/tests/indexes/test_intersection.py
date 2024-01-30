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


class IntersectionMixin:
    def test_intersection(self):
        pidx = pd.Index([1, 2, 3, 4], name="Koalas")
        psidx = ps.from_pandas(pidx)

        # other = Index
        pidx_other = pd.Index([3, 4, 5, 6], name="Koalas")
        psidx_other = ps.from_pandas(pidx_other)
        self.assert_eq(pidx.intersection(pidx_other), psidx.intersection(psidx_other).sort_values())
        self.assert_eq(
            (pidx + 1).intersection(pidx_other), (psidx + 1).intersection(psidx_other).sort_values()
        )

        pidx_other_different_name = pd.Index([3, 4, 5, 6], name="Databricks")
        psidx_other_different_name = ps.from_pandas(pidx_other_different_name)
        self.assert_eq(
            pidx.intersection(pidx_other_different_name),
            psidx.intersection(psidx_other_different_name).sort_values(),
        )
        self.assert_eq(
            (pidx + 1).intersection(pidx_other_different_name),
            (psidx + 1).intersection(psidx_other_different_name).sort_values(),
        )

        pidx_other_from_frame = pd.DataFrame({"a": [3, 4, 5, 6]}).set_index("a").index
        psidx_other_from_frame = ps.from_pandas(pidx_other_from_frame)
        self.assert_eq(
            pidx.intersection(pidx_other_from_frame),
            psidx.intersection(psidx_other_from_frame).sort_values(),
        )
        self.assert_eq(
            (pidx + 1).intersection(pidx_other_from_frame),
            (psidx + 1).intersection(psidx_other_from_frame).sort_values(),
        )

        # other = MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psmidx = ps.from_pandas(pmidx)
        self.assert_eq(
            pidx.intersection(pmidx), psidx.intersection(psmidx).sort_values(), almost=True
        )
        self.assert_eq(
            (pidx + 1).intersection(pmidx),
            (psidx + 1).intersection(psmidx).sort_values(),
            almost=True,
        )

        # other = Series
        pser = pd.Series([3, 4, 5, 6])
        psser = ps.from_pandas(pser)
        self.assert_eq(pidx.intersection(pser), psidx.intersection(psser).sort_values())
        self.assert_eq((pidx + 1).intersection(pser), (psidx + 1).intersection(psser).sort_values())

        pser_different_name = pd.Series([3, 4, 5, 6], name="Databricks")
        psser_different_name = ps.from_pandas(pser_different_name)
        self.assert_eq(
            pidx.intersection(pser_different_name),
            psidx.intersection(psser_different_name).sort_values(),
        )
        self.assert_eq(
            (pidx + 1).intersection(pser_different_name),
            (psidx + 1).intersection(psser_different_name).sort_values(),
        )

        others = ([3, 4, 5, 6], (3, 4, 5, 6), {3: None, 4: None, 5: None, 6: None})
        for other in others:
            self.assert_eq(pidx.intersection(other), psidx.intersection(other).sort_values())
            self.assert_eq(
                (pidx + 1).intersection(other), (psidx + 1).intersection(other).sort_values()
            )

        # MultiIndex / other = Index
        self.assert_eq(
            pmidx.intersection(pidx), psmidx.intersection(psidx).sort_values(), almost=True
        )
        self.assert_eq(
            pmidx.intersection(pidx_other_from_frame),
            psmidx.intersection(psidx_other_from_frame).sort_values(),
            almost=True,
        )

        # MultiIndex / other = MultiIndex
        pmidx_other = pd.MultiIndex.from_tuples([("c", "z"), ("d", "w")])
        psmidx_other = ps.from_pandas(pmidx_other)
        self.assert_eq(
            pmidx.intersection(pmidx_other), psmidx.intersection(psmidx_other).sort_values()
        )

        # MultiIndex / other = list
        other = [("c", "z"), ("d", "w")]
        self.assert_eq(pmidx.intersection(other), psmidx.intersection(other).sort_values())

        # MultiIndex / other = tuple
        other = (("c", "z"), ("d", "w"))
        self.assert_eq(pmidx.intersection(other), psmidx.intersection(other).sort_values())

        # MultiIndex / other = dict
        other = {("c", "z"): None, ("d", "w"): None}
        self.assert_eq(pmidx.intersection(other), psmidx.intersection(other).sort_values())

        # MultiIndex with different names.
        pmidx1 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")], names=["X", "Y"])
        pmidx2 = pd.MultiIndex.from_tuples([("c", "z"), ("d", "w")], names=["A", "B"])
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.intersection(pmidx2), psmidx1.intersection(psmidx2).sort_values())

        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            psidx.intersection(4)
        with self.assertRaisesRegex(TypeError, "other must be a MultiIndex or a list of tuples"):
            psmidx.intersection(4)
        with self.assertRaisesRegex(TypeError, "other must be a MultiIndex or a list of tuples"):
            psmidx.intersection(ps.Series([3, 4, 5, 6]))
        with self.assertRaisesRegex(TypeError, "other must be a MultiIndex or a list of tuples"):
            psmidx.intersection([("c", "z"), ["d", "w"]])
        with self.assertRaisesRegex(ValueError, "Index data must be 1-dimensional"):
            psidx.intersection(ps.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}))
        with self.assertRaisesRegex(ValueError, "Index data must be 1-dimensional"):
            psmidx.intersection(ps.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}))
        # other = list of tuple
        with self.assertRaisesRegex(ValueError, "Names should be list-like for a MultiIndex"):
            psidx.intersection([(1, 2), (3, 4)])


class IntersectionTests(
    IntersectionMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_intersection import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
