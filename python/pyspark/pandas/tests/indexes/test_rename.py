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

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class FrameRenameMixin:
    def test_rename_dataframe(self):
        pdf1 = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
        psdf1 = ps.from_pandas(pdf1)

        self.assert_eq(
            psdf1.rename(columns={"A": "a", "B": "b"}), pdf1.rename(columns={"A": "a", "B": "b"})
        )

        result_psdf = psdf1.rename(index={1: 10, 2: 20})
        result_pdf = pdf1.rename(index={1: 10, 2: 20})
        self.assert_eq(result_psdf, result_pdf)

        # inplace
        pser = result_pdf.A
        psser = result_psdf.A
        result_psdf.rename(index={10: 100, 20: 200}, inplace=True)
        result_pdf.rename(index={10: 100, 20: 200}, inplace=True)
        self.assert_eq(result_psdf, result_pdf)
        self.assert_eq(psser, pser)

        def str_lower(s) -> str:
            return str.lower(s)

        self.assert_eq(
            psdf1.rename(str_lower, axis="columns"), pdf1.rename(str_lower, axis="columns")
        )

        def mul10(x) -> int:
            return x * 10

        self.assert_eq(psdf1.rename(mul10, axis="index"), pdf1.rename(mul10, axis="index"))

        self.assert_eq(
            psdf1.rename(columns=str_lower, index={1: 10, 2: 20}),
            pdf1.rename(columns=str_lower, index={1: 10, 2: 20}),
        )

        self.assert_eq(
            psdf1.rename(columns=lambda x: str.lower(x)),
            pdf1.rename(columns=lambda x: str.lower(x)),
        )

        idx = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C"), ("Y", "D")])
        pdf2 = pd.DataFrame([[1, 2, 3, 4], [5, 6, 7, 8]], columns=idx)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(psdf2.rename(columns=str_lower), pdf2.rename(columns=str_lower))
        self.assert_eq(
            psdf2.rename(columns=lambda x: str.lower(x)),
            pdf2.rename(columns=lambda x: str.lower(x)),
        )

        self.assert_eq(
            psdf2.rename(columns=str_lower, level=0), pdf2.rename(columns=str_lower, level=0)
        )
        self.assert_eq(
            psdf2.rename(columns=str_lower, level=1), pdf2.rename(columns=str_lower, level=1)
        )

        pdf3 = pd.DataFrame([[1, 2], [3, 4], [5, 6], [7, 8]], index=idx, columns=list("ab"))
        psdf3 = ps.from_pandas(pdf3)

        self.assert_eq(psdf3.rename(index=str_lower), pdf3.rename(index=str_lower))
        self.assert_eq(
            psdf3.rename(index=str_lower, level=0), pdf3.rename(index=str_lower, level=0)
        )
        self.assert_eq(
            psdf3.rename(index=str_lower, level=1), pdf3.rename(index=str_lower, level=1)
        )

        pdf4 = pdf2 + 1
        psdf4 = psdf2 + 1
        self.assert_eq(psdf4.rename(columns=str_lower), pdf4.rename(columns=str_lower))

        pdf5 = pdf3 + 1
        psdf5 = psdf3 + 1
        self.assert_eq(psdf5.rename(index=str_lower), pdf5.rename(index=str_lower))

        msg = "Either `index` or `columns` should be provided."
        with self.assertRaisesRegex(ValueError, msg):
            psdf1.rename()
        msg = "`mapper` or `index` or `columns` should be either dict-like or function type."
        with self.assertRaisesRegex(ValueError, msg):
            psdf1.rename(mapper=[str_lower], axis=1)
        msg = "Mapper dict should have the same value type."
        with self.assertRaisesRegex(ValueError, msg):
            psdf1.rename({"A": "a", "B": 2}, axis=1)
        msg = r"level should be an integer between \[0, column_labels_level\)"
        with self.assertRaisesRegex(ValueError, msg):
            psdf2.rename(columns=str_lower, level=2)
        msg = r"level should be an integer between \[0, 2\)"
        with self.assertRaisesRegex(ValueError, msg):
            psdf3.rename(index=str_lower, level=2)

    def test_rename_axis(self):
        index = pd.Index(["A", "B", "C"], name="index")
        columns = pd.Index(["numbers", "values"], name="cols")
        pdf = pd.DataFrame([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]], index=index, columns=columns)
        psdf = ps.from_pandas(pdf)

        for axis in [0, "index"]:
            self.assert_eq(
                pdf.rename_axis("index2", axis=axis).sort_index(),
                psdf.rename_axis("index2", axis=axis).sort_index(),
            )
            self.assert_eq(
                pdf.rename_axis(["index2"], axis=axis).sort_index(),
                psdf.rename_axis(["index2"], axis=axis).sort_index(),
            )

        for axis in [1, "columns"]:
            self.assert_eq(
                pdf.rename_axis("cols2", axis=axis).sort_index(),
                psdf.rename_axis("cols2", axis=axis).sort_index(),
            )
            self.assert_eq(
                pdf.rename_axis(["cols2"], axis=axis).sort_index(),
                psdf.rename_axis(["cols2"], axis=axis).sort_index(),
            )

        pdf2 = pdf.copy()
        psdf2 = psdf.copy()
        pdf2.rename_axis("index2", axis="index", inplace=True)
        psdf2.rename_axis("index2", axis="index", inplace=True)
        self.assert_eq(pdf2.sort_index(), psdf2.sort_index())

        self.assertRaises(ValueError, lambda: psdf.rename_axis(["index2", "index3"], axis=0))
        self.assertRaises(ValueError, lambda: psdf.rename_axis(["cols2", "cols3"], axis=1))
        self.assertRaises(TypeError, lambda: psdf.rename_axis(mapper=["index2"], index=["index3"]))
        self.assertRaises(ValueError, lambda: psdf.rename_axis(ps))

        self.assert_eq(
            pdf.rename_axis(index={"index": "index2"}, columns={"cols": "cols2"}).sort_index(),
            psdf.rename_axis(index={"index": "index2"}, columns={"cols": "cols2"}).sort_index(),
        )

        self.assert_eq(
            pdf.rename_axis(index={"missing": "index2"}, columns={"missing": "cols2"}).sort_index(),
            psdf.rename_axis(
                index={"missing": "index2"}, columns={"missing": "cols2"}
            ).sort_index(),
        )

        self.assert_eq(
            pdf.rename_axis(index=str.upper, columns=str.upper).sort_index(),
            psdf.rename_axis(index=str.upper, columns=str.upper).sort_index(),
        )

        index = pd.MultiIndex.from_tuples(
            [("A", "B"), ("C", "D"), ("E", "F")], names=["index1", "index2"]
        )
        columns = pd.MultiIndex.from_tuples(
            [("numbers", "first"), ("values", "second")], names=["cols1", "cols2"]
        )
        pdf = pd.DataFrame([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]], index=index, columns=columns)
        psdf = ps.from_pandas(pdf)

        for axis in [0, "index"]:
            self.assert_eq(
                pdf.rename_axis(["index3", "index4"], axis=axis).sort_index(),
                psdf.rename_axis(["index3", "index4"], axis=axis).sort_index(),
            )

        for axis in [1, "columns"]:
            self.assert_eq(
                pdf.rename_axis(["cols3", "cols4"], axis=axis).sort_index(),
                psdf.rename_axis(["cols3", "cols4"], axis=axis).sort_index(),
            )

        self.assertRaises(
            ValueError, lambda: psdf.rename_axis(["index3", "index4", "index5"], axis=0)
        )
        self.assertRaises(ValueError, lambda: psdf.rename_axis(["cols3", "cols4", "cols5"], axis=1))

        self.assert_eq(
            pdf.rename_axis(index={"index1": "index3"}, columns={"cols1": "cols3"}).sort_index(),
            psdf.rename_axis(index={"index1": "index3"}, columns={"cols1": "cols3"}).sort_index(),
        )

        self.assert_eq(
            pdf.rename_axis(index={"missing": "index3"}, columns={"missing": "cols3"}).sort_index(),
            psdf.rename_axis(
                index={"missing": "index3"}, columns={"missing": "cols3"}
            ).sort_index(),
        )

        self.assert_eq(
            pdf.rename_axis(
                index={"index1": "index3", "index2": "index4"},
                columns={"cols1": "cols3", "cols2": "cols4"},
            ).sort_index(),
            psdf.rename_axis(
                index={"index1": "index3", "index2": "index4"},
                columns={"cols1": "cols3", "cols2": "cols4"},
            ).sort_index(),
        )

        self.assert_eq(
            pdf.rename_axis(index=str.upper, columns=str.upper).sort_index(),
            psdf.rename_axis(index=str.upper, columns=str.upper).sort_index(),
        )

    def test_multi_index_rename(self):
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        idx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame(np.random.randn(4, 5), idx)
        psdf = ps.from_pandas(pdf)

        pmidx = pdf.index
        psmidx = psdf.index

        self.assert_eq(psmidx.rename(["n", "c"]), pmidx.rename(["n", "c"]))
        self.assert_eq(psdf.index.names, pdf.index.names)

        # non-string names
        self.assert_eq(psmidx.rename([0, 1]), pmidx.rename([0, 1]))
        self.assert_eq(
            psmidx.rename([("x", "a"), ("y", "b")]), pmidx.rename([("x", "a"), ("y", "b")])
        )

        psmidx.rename(["num", "col"], inplace=True)
        pmidx.rename(["num", "col"], inplace=True)

        self.assert_eq(psmidx, pmidx)
        self.assert_eq(psdf.index.names, pdf.index.names)

        self.assert_eq(psmidx.rename([None, None]), pmidx.rename([None, None]))
        self.assert_eq(psdf.index.names, pdf.index.names)

        self.assertRaises(TypeError, lambda: psmidx.rename("number"))
        self.assertRaises(TypeError, lambda: psmidx.rename(None))
        self.assertRaises(ValueError, lambda: psmidx.rename(["number"]))

    def test_multiindex_rename(self):
        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        psidx = ps.from_pandas(pidx)

        pidx = pidx.rename(list("ABC"))
        psidx = psidx.rename(list("ABC"))
        self.assert_eq(pidx, psidx)

        pidx = pidx.rename(["my", "name", "is"])
        psidx = psidx.rename(["my", "name", "is"])
        self.assert_eq(pidx, psidx)

    def test_index_rename(self):
        pdf = pd.DataFrame(
            np.random.randn(10, 5), index=pd.Index([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], name="x")
        )
        psdf = ps.from_pandas(pdf)

        pidx = pdf.index
        psidx = psdf.index

        self.assert_eq(psidx.rename("y"), pidx.rename("y"))
        self.assert_eq(psdf.index.names, pdf.index.names)

        # non-string names
        self.assert_eq(psidx.rename(0), pidx.rename(0))
        self.assert_eq(psidx.rename(("y", 0)), pidx.rename(("y", 0)))

        psidx.rename("z", inplace=True)
        pidx.rename("z", inplace=True)

        self.assert_eq(psidx, pidx)
        self.assert_eq(psdf.index.names, pdf.index.names)

        self.assert_eq(psidx.rename(None), pidx.rename(None))
        self.assert_eq(psdf.index.names, pdf.index.names)

        self.assertRaises(TypeError, lambda: psidx.rename(["x", "y"]))


class FrameRenameTests(
    FrameRenameMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_rename import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
