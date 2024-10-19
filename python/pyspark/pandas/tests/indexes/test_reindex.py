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


class FrameReindexMixin:
    def test_reindex(self):
        index = pd.Index(["A", "B", "C", "D", "E"])
        columns = pd.Index(["numbers"])
        pdf = pd.DataFrame([1.0, 2.0, 3.0, 4.0, None], index=index, columns=columns)
        psdf = ps.from_pandas(pdf)

        columns2 = pd.Index(["numbers", "2", "3"], name="cols2")
        self.assert_eq(
            pdf.reindex(columns=columns2).sort_index(),
            psdf.reindex(columns=columns2).sort_index(),
        )

        columns = pd.Index(["numbers"], name="cols")
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            pdf.reindex(["A", "B", "C"], columns=["numbers", "2", "3"]).sort_index(),
            psdf.reindex(["A", "B", "C"], columns=["numbers", "2", "3"]).sort_index(),
        )

        # We manually test this due to the bug in pandas.
        expected_result = ps.DataFrame([1.0, 2.0, 3.0], index=ps.Index(["A", "B", "C"]))
        expected_result.columns = pd.Index(["numbers"], name="cols")
        self.assert_eq(
            psdf.reindex(["A", "B", "C"], index=["numbers", "2", "3"]).sort_index(),
            expected_result,
        )

        self.assert_eq(
            pdf.reindex(index=["A", "B"]).sort_index(), psdf.reindex(index=["A", "B"]).sort_index()
        )

        self.assert_eq(
            pdf.reindex(index=["A", "B", "2", "3"]).sort_index(),
            psdf.reindex(index=["A", "B", "2", "3"]).sort_index(),
        )

        self.assert_eq(
            pdf.reindex(index=["A", "E", "2", "3"], fill_value=0).sort_index(),
            psdf.reindex(index=["A", "E", "2", "3"], fill_value=0).sort_index(),
        )

        self.assert_eq(
            pdf.reindex(columns=["numbers"]).sort_index(),
            psdf.reindex(columns=["numbers"]).sort_index(),
        )

        self.assert_eq(
            pdf.reindex(columns=["numbers"], copy=True).sort_index(),
            psdf.reindex(columns=["numbers"], copy=True).sort_index(),
        )

        # Using float as fill_value to avoid int64/32 clash
        self.assert_eq(
            pdf.reindex(columns=["numbers", "2", "3"], fill_value=0.0).sort_index(),
            psdf.reindex(columns=["numbers", "2", "3"], fill_value=0.0).sort_index(),
        )

        columns2 = pd.Index(["numbers", "2", "3"])
        self.assert_eq(
            pdf.reindex(columns=columns2).sort_index(),
            psdf.reindex(columns=columns2).sort_index(),
        )

        columns2 = pd.Index(["numbers", "2", "3"], name="cols2")
        self.assert_eq(
            pdf.reindex(columns=columns2).sort_index(),
            psdf.reindex(columns=columns2).sort_index(),
        )

        # Reindexing single Index on single Index
        pindex2 = pd.Index(["A", "C", "D", "E", "0"], name="index2")
        kindex2 = ps.from_pandas(pindex2)

        for fill_value in [None, 0]:
            self.assert_eq(
                pdf.reindex(index=pindex2, fill_value=fill_value).sort_index(),
                psdf.reindex(index=kindex2, fill_value=fill_value).sort_index(),
            )

        pindex2 = pd.DataFrame({"index2": ["A", "C", "D", "E", "0"]}).set_index("index2").index
        kindex2 = ps.from_pandas(pindex2)

        for fill_value in [None, 0]:
            self.assert_eq(
                pdf.reindex(index=pindex2, fill_value=fill_value).sort_index(),
                psdf.reindex(index=kindex2, fill_value=fill_value).sort_index(),
            )

        # Reindexing MultiIndex on single Index
        pindex = pd.MultiIndex.from_tuples(
            [("A", "B"), ("C", "D"), ("F", "G")], names=["name1", "name2"]
        )
        kindex = ps.from_pandas(pindex)

        self.assert_eq(
            pdf.reindex(index=pindex, fill_value=0.0).sort_index(),
            psdf.reindex(index=kindex, fill_value=0.0).sort_index(),
        )

        # Specifying the `labels` parameter
        new_index = ["V", "W", "X", "Y", "Z"]
        self.assert_eq(
            pdf.reindex(labels=new_index, fill_value=0.0, axis=0).sort_index(),
            psdf.reindex(labels=new_index, fill_value=0.0, axis=0).sort_index(),
        )
        self.assert_eq(
            pdf.reindex(labels=new_index, fill_value=0.0, axis=1).sort_index(),
            psdf.reindex(labels=new_index, fill_value=0.0, axis=1).sort_index(),
        )

        self.assertRaises(TypeError, lambda: psdf.reindex(columns=["numbers", "2", "3"], axis=1))
        self.assertRaises(TypeError, lambda: psdf.reindex(columns=["numbers", "2", "3"], axis=2))
        self.assertRaises(TypeError, lambda: psdf.reindex(columns="numbers"))
        self.assertRaises(TypeError, lambda: psdf.reindex(index=["A", "B", "C"], axis=1))
        self.assertRaises(TypeError, lambda: psdf.reindex(index=123))

        # Reindexing MultiIndex on MultiIndex
        pdf = pd.DataFrame({"numbers": [1.0, 2.0, None]}, index=pindex)
        psdf = ps.from_pandas(pdf)
        pindex2 = pd.MultiIndex.from_tuples(
            [("A", "G"), ("C", "D"), ("I", "J")], names=["name1", "name2"]
        )
        kindex2 = ps.from_pandas(pindex2)

        for fill_value in [None, 0.0]:
            self.assert_eq(
                pdf.reindex(index=pindex2, fill_value=fill_value).sort_index(),
                psdf.reindex(index=kindex2, fill_value=fill_value).sort_index(),
            )

        pindex2 = (
            pd.DataFrame({"index_level_1": ["A", "C", "I"], "index_level_2": ["G", "D", "J"]})
            .set_index(["index_level_1", "index_level_2"])
            .index
        )
        kindex2 = ps.from_pandas(pindex2)

        for fill_value in [None, 0.0]:
            self.assert_eq(
                pdf.reindex(index=pindex2, fill_value=fill_value).sort_index(),
                psdf.reindex(index=kindex2, fill_value=fill_value).sort_index(),
            )

        columns = pd.MultiIndex.from_tuples([("X", "numbers")], names=["cols1", "cols2"])
        pdf.columns = columns
        psdf.columns = columns

        # Reindexing MultiIndex index on MultiIndex columns and MultiIndex index
        for fill_value in [None, 0.0]:
            self.assert_eq(
                pdf.reindex(index=pindex2, fill_value=fill_value).sort_index(),
                psdf.reindex(index=kindex2, fill_value=fill_value).sort_index(),
            )

        index = pd.Index(["A", "B", "C", "D", "E"])
        pdf = pd.DataFrame(data=[1.0, 2.0, 3.0, 4.0, None], index=index, columns=columns)
        psdf = ps.from_pandas(pdf)
        pindex2 = pd.Index(["A", "C", "D", "E", "0"], name="index2")
        kindex2 = ps.from_pandas(pindex2)

        # Reindexing single Index on MultiIndex columns and single Index
        for fill_value in [None, 0.0]:
            self.assert_eq(
                pdf.reindex(index=pindex2, fill_value=fill_value).sort_index(),
                psdf.reindex(index=kindex2, fill_value=fill_value).sort_index(),
            )

        for fill_value in [None, 0.0]:
            self.assert_eq(
                pdf.reindex(
                    columns=[("X", "numbers"), ("Y", "2"), ("Y", "3")], fill_value=fill_value
                ).sort_index(),
                psdf.reindex(
                    columns=[("X", "numbers"), ("Y", "2"), ("Y", "3")], fill_value=fill_value
                ).sort_index(),
            )

        columns2 = pd.MultiIndex.from_tuples(
            [("X", "numbers"), ("Y", "2"), ("Y", "3")], names=["cols3", "cols4"]
        )
        self.assert_eq(
            pdf.reindex(columns=columns2).sort_index(),
            psdf.reindex(columns=columns2).sort_index(),
        )

        self.assertRaises(TypeError, lambda: psdf.reindex(columns=["X"]))
        self.assertRaises(ValueError, lambda: psdf.reindex(columns=[("X",)]))

    def test_reindex_like(self):
        data = [[1.0, 2.0], [3.0, None], [None, 4.0]]
        index = pd.Index(["A", "B", "C"], name="index")
        columns = pd.Index(["numbers", "values"], name="cols")
        pdf = pd.DataFrame(data=data, index=index, columns=columns)
        psdf = ps.from_pandas(pdf)

        # Reindexing single Index on single Index
        data2 = [[5.0, None], [6.0, 7.0], [8.0, None]]
        index2 = pd.Index(["A", "C", "D"], name="index2")
        columns2 = pd.Index(["numbers", "F"], name="cols2")
        pdf2 = pd.DataFrame(data=data2, index=index2, columns=columns2)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(
            pdf.reindex_like(pdf2).sort_index(),
            psdf.reindex_like(psdf2).sort_index(),
        )

        pdf2 = pd.DataFrame({"index_level_1": ["A", "C", "I"]})
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(
            pdf.reindex_like(pdf2.set_index(["index_level_1"])).sort_index(),
            psdf.reindex_like(psdf2.set_index(["index_level_1"])).sort_index(),
        )

        # Reindexing MultiIndex on single Index
        index2 = pd.MultiIndex.from_tuples(
            [("A", "G"), ("C", "D"), ("I", "J")], names=["name3", "name4"]
        )
        pdf2 = pd.DataFrame(data=data2, index=index2)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(
            pdf.reindex_like(pdf2).sort_index(),
            psdf.reindex_like(psdf2).sort_index(),
        )

        self.assertRaises(TypeError, lambda: psdf.reindex_like(index2))
        self.assertRaises(AssertionError, lambda: psdf2.reindex_like(psdf))

        # Reindexing MultiIndex on MultiIndex
        columns2 = pd.MultiIndex.from_tuples(
            [("numbers", "third"), ("values", "second")], names=["cols3", "cols4"]
        )
        pdf2.columns = columns2
        psdf2.columns = columns2

        columns = pd.MultiIndex.from_tuples(
            [("numbers", "first"), ("values", "second")], names=["cols1", "cols2"]
        )
        index = pd.MultiIndex.from_tuples(
            [("A", "B"), ("C", "D"), ("E", "F")], names=["name1", "name2"]
        )
        pdf = pd.DataFrame(data=data, index=index, columns=columns)
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.reindex_like(pdf2).sort_index(),
            psdf.reindex_like(psdf2).sort_index(),
        )


class FrameReindexTests(
    FrameReindexMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_reindex import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
