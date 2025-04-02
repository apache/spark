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

from pyspark import pandas as ps
from pyspark.pandas.config import set_option, reset_option, option_context
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class ReprTestsMixin:
    max_display_count = 23

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("display.max_rows", ReprTests.max_display_count)

    @classmethod
    def tearDownClass(cls):
        reset_option("display.max_rows")
        super().tearDownClass()

    def test_repr_dataframe(self):
        psdf = ps.range(ReprTests.max_display_count)
        self.assertTrue("Showing only the first" not in repr(psdf))
        self.assert_eq(repr(psdf), repr(psdf._to_pandas()))

        psdf = ps.range(ReprTests.max_display_count + 1)
        self.assertTrue("Showing only the first" in repr(psdf))
        self.assertTrue(
            repr(psdf).startswith(repr(psdf._to_pandas().head(ReprTests.max_display_count)))
        )

        with option_context("display.max_rows", None):
            psdf = ps.range(ReprTests.max_display_count + 1)
            self.assert_eq(repr(psdf), repr(psdf._to_pandas()))

    def test_repr_series(self):
        psser = ps.range(ReprTests.max_display_count).id
        self.assertTrue("Showing only the first" not in repr(psser))
        self.assert_eq(repr(psser), repr(psser._to_pandas()))

        psser = ps.range(ReprTests.max_display_count + 1).id
        self.assertTrue("Showing only the first" in repr(psser))
        self.assertTrue(
            repr(psser).startswith(repr(psser._to_pandas().head(ReprTests.max_display_count)))
        )

        with option_context("display.max_rows", None):
            psser = ps.range(ReprTests.max_display_count + 1).id
            self.assert_eq(repr(psser), repr(psser._to_pandas()))

        psser = ps.range(ReprTests.max_display_count).id.rename()
        self.assertTrue("Showing only the first" not in repr(psser))
        self.assert_eq(repr(psser), repr(psser._to_pandas()))

        psser = ps.range(ReprTests.max_display_count + 1).id.rename()
        self.assertTrue("Showing only the first" in repr(psser))
        self.assertTrue(
            repr(psser).startswith(repr(psser._to_pandas().head(ReprTests.max_display_count)))
        )

        with option_context("display.max_rows", None):
            psser = ps.range(ReprTests.max_display_count + 1).id.rename()
            self.assert_eq(repr(psser), repr(psser._to_pandas()))

        psser = ps.MultiIndex.from_tuples(
            [(100 * i, i) for i in range(ReprTests.max_display_count)]
        ).to_series()
        self.assertTrue("Showing only the first" not in repr(psser))
        self.assert_eq(repr(psser), repr(psser._to_pandas()))

        psser = ps.MultiIndex.from_tuples(
            [(100 * i, i) for i in range(ReprTests.max_display_count + 1)]
        ).to_series()
        self.assertTrue("Showing only the first" in repr(psser))
        self.assertTrue(
            repr(psser).startswith(repr(psser._to_pandas().head(ReprTests.max_display_count)))
        )

        with option_context("display.max_rows", None):
            psser = ps.MultiIndex.from_tuples(
                [(100 * i, i) for i in range(ReprTests.max_display_count + 1)]
            ).to_series()
            self.assert_eq(repr(psser), repr(psser._to_pandas()))

    def test_repr_indexes(self):
        psidx = ps.range(ReprTests.max_display_count).index
        self.assertTrue("Showing only the first" not in repr(psidx))
        self.assert_eq(repr(psidx), repr(psidx._to_pandas()))

        psidx = ps.range(ReprTests.max_display_count + 1).index
        self.assertTrue("Showing only the first" in repr(psidx))
        self.assertTrue(
            repr(psidx).startswith(
                repr(psidx._to_pandas().to_series().head(ReprTests.max_display_count).index)
            )
        )

        with option_context("display.max_rows", None):
            psidx = ps.range(ReprTests.max_display_count + 1).index
            self.assert_eq(repr(psidx), repr(psidx._to_pandas()))

        psidx = ps.MultiIndex.from_tuples(
            [(100 * i, i) for i in range(ReprTests.max_display_count)]
        )
        self.assertTrue("Showing only the first" not in repr(psidx))
        self.assert_eq(repr(psidx), repr(psidx._to_pandas()))

        psidx = ps.MultiIndex.from_tuples(
            [(100 * i, i) for i in range(ReprTests.max_display_count + 1)]
        )
        self.assertTrue("Showing only the first" in repr(psidx))
        self.assertTrue(
            repr(psidx).startswith(
                repr(psidx._to_pandas().to_frame().head(ReprTests.max_display_count).index)
            )
        )

        with option_context("display.max_rows", None):
            psidx = ps.MultiIndex.from_tuples(
                [(100 * i, i) for i in range(ReprTests.max_display_count + 1)]
            )
            self.assert_eq(repr(psidx), repr(psidx._to_pandas()))

    def test_html_repr(self):
        psdf = ps.range(ReprTests.max_display_count)
        self.assertTrue("Showing only the first" not in psdf._repr_html_())
        self.assertEqual(psdf._repr_html_(), psdf._to_pandas()._repr_html_())

        psdf = ps.range(ReprTests.max_display_count + 1)
        self.assertTrue("Showing only the first" in psdf._repr_html_())

        with option_context("display.max_rows", None):
            psdf = ps.range(ReprTests.max_display_count + 1)
            self.assertEqual(psdf._repr_html_(), psdf._to_pandas()._repr_html_())

    def test_repr_float_index(self):
        psdf = ps.DataFrame(
            {"a": np.random.rand(ReprTests.max_display_count)},
            index=np.random.rand(ReprTests.max_display_count),
        )
        self.assertTrue("Showing only the first" not in repr(psdf))
        self.assert_eq(repr(psdf), repr(psdf._to_pandas()))
        self.assertTrue("Showing only the first" not in repr(psdf.a))
        self.assert_eq(repr(psdf.a), repr(psdf.a._to_pandas()))
        self.assertTrue("Showing only the first" not in repr(psdf.index))
        self.assert_eq(repr(psdf.index), repr(psdf.index._to_pandas()))

        self.assertTrue("Showing only the first" not in psdf._repr_html_())
        self.assertEqual(psdf._repr_html_(), psdf._to_pandas()._repr_html_())

        psdf = ps.DataFrame(
            {"a": np.random.rand(ReprTests.max_display_count + 1)},
            index=np.random.rand(ReprTests.max_display_count + 1),
        )
        self.assertTrue("Showing only the first" in repr(psdf))
        self.assertTrue("Showing only the first" in repr(psdf.a))
        self.assertTrue("Showing only the first" in repr(psdf.index))
        self.assertTrue("Showing only the first" in psdf._repr_html_())


class ReprTests(ReprTestsMixin, PandasOnSparkTestCase):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_repr import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
