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
from pyspark.pandas.groupby import SeriesGroupBy


class GroupingTestsMixin:
    def test_get_group(self):
        pdf = pd.DataFrame(
            [
                ("falcon", "bird", 389.0),
                ("parrot", "bird", 24.0),
                ("lion", "mammal", 80.5),
                ("monkey", "mammal", np.nan),
            ],
            columns=["name", "class", "max_speed"],
            index=[0, 2, 3, 1],
        )
        pdf.columns.name = "Koalas"
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("class").get_group("bird"),
            pdf.groupby("class").get_group("bird"),
        )
        self.assert_eq(
            psdf.groupby("class")["name"].get_group("mammal"),
            pdf.groupby("class")["name"].get_group("mammal"),
        )
        self.assert_eq(
            psdf.groupby("class")[["name"]].get_group("mammal"),
            pdf.groupby("class")[["name"]].get_group("mammal"),
        )
        self.assert_eq(
            psdf.groupby(["class", "name"]).get_group(("mammal", "lion")),
            pdf.groupby(["class", "name"]).get_group(("mammal", "lion")),
        )
        self.assert_eq(
            psdf.groupby(["class", "name"])["max_speed"].get_group(("mammal", "lion")),
            pdf.groupby(["class", "name"])["max_speed"].get_group(("mammal", "lion")),
        )
        self.assert_eq(
            psdf.groupby(["class", "name"])[["max_speed"]].get_group(("mammal", "lion")),
            pdf.groupby(["class", "name"])[["max_speed"]].get_group(("mammal", "lion")),
        )
        self.assert_eq(
            (psdf.max_speed + 1).groupby(psdf["class"]).get_group("mammal"),
            (pdf.max_speed + 1).groupby(pdf["class"]).get_group("mammal"),
        )
        self.assert_eq(
            psdf.groupby("max_speed").get_group(80.5),
            pdf.groupby("max_speed").get_group(80.5),
        )

        self.assertRaises(KeyError, lambda: psdf.groupby("class").get_group("fish"))
        self.assertRaises(TypeError, lambda: psdf.groupby("class").get_group(["bird", "mammal"]))
        self.assertRaises(KeyError, lambda: psdf.groupby("class")["name"].get_group("fish"))
        self.assertRaises(
            TypeError, lambda: psdf.groupby("class")["name"].get_group(["bird", "mammal"])
        )
        self.assertRaises(
            KeyError, lambda: psdf.groupby(["class", "name"]).get_group(("lion", "mammal"))
        )
        self.assertRaises(ValueError, lambda: psdf.groupby(["class", "name"]).get_group(("lion",)))
        self.assertRaises(
            ValueError, lambda: psdf.groupby(["class", "name"]).get_group(("mammal",))
        )
        self.assertRaises(ValueError, lambda: psdf.groupby(["class", "name"]).get_group("mammal"))

        # MultiIndex columns
        pdf.columns = pd.MultiIndex.from_tuples([("A", "name"), ("B", "class"), ("C", "max_speed")])
        pdf.columns.names = ["Hello", "Koalas"]
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            psdf.groupby(("B", "class")).get_group("bird"),
            pdf.groupby(("B", "class")).get_group("bird"),
        )
        self.assert_eq(
            psdf.groupby(("B", "class"))[[("A", "name")]].get_group("mammal"),
            pdf.groupby(("B", "class"))[[("A", "name")]].get_group("mammal"),
        )
        self.assert_eq(
            psdf.groupby([("B", "class"), ("A", "name")]).get_group(("mammal", "lion")),
            pdf.groupby([("B", "class"), ("A", "name")]).get_group(("mammal", "lion")),
        )
        self.assert_eq(
            psdf.groupby([("B", "class"), ("A", "name")])[[("C", "max_speed")]].get_group(
                ("mammal", "lion")
            ),
            pdf.groupby([("B", "class"), ("A", "name")])[[("C", "max_speed")]].get_group(
                ("mammal", "lion")
            ),
        )
        self.assert_eq(
            (psdf[("C", "max_speed")] + 1).groupby(psdf[("B", "class")]).get_group("mammal"),
            (pdf[("C", "max_speed")] + 1).groupby(pdf[("B", "class")]).get_group("mammal"),
        )
        self.assert_eq(
            psdf.groupby(("C", "max_speed")).get_group(80.5),
            pdf.groupby(("C", "max_speed")).get_group(80.5),
        )

        self.assertRaises(KeyError, lambda: psdf.groupby(("B", "class")).get_group("fish"))
        self.assertRaises(
            TypeError, lambda: psdf.groupby(("B", "class")).get_group(["bird", "mammal"])
        )
        self.assertRaises(
            KeyError, lambda: psdf.groupby(("B", "class"))[("A", "name")].get_group("fish")
        )
        self.assertRaises(
            KeyError,
            lambda: psdf.groupby([("B", "class"), ("A", "name")]).get_group(("lion", "mammal")),
        )
        self.assertRaises(
            ValueError,
            lambda: psdf.groupby([("B", "class"), ("A", "name")]).get_group(("lion",)),
        )
        self.assertRaises(
            ValueError, lambda: psdf.groupby([("B", "class"), ("A", "name")]).get_group(("mammal",))
        )
        self.assertRaises(
            ValueError, lambda: psdf.groupby([("B", "class"), ("A", "name")]).get_group("mammal")
        )

    def test_getitem(self):
        psdf = ps.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3] * 3,
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5] * 3,
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6] * 3,
            },
            index=np.random.rand(10 * 3),
        )

        self.assertTrue(isinstance(psdf.groupby("a")["b"], SeriesGroupBy))


class GroupingTests(
    GroupingTestsMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_grouping import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
