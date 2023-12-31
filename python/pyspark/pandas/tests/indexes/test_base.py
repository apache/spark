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

import inspect
import unittest
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import pyspark.pandas as ps
from pyspark.loose_version import LooseVersion
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.pandas.missing.indexes import (
    MissingPandasLikeDatetimeIndex,
    MissingPandasLikeIndex,
    MissingPandasLikeMultiIndex,
    MissingPandasLikeTimedeltaIndex,
)
from pyspark.testing.pandasutils import ComparisonTestBase, TestUtils, SPARK_CONF_ARROW_ENABLED


class IndexesTestsMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )

    def test_index_basic(self):
        for pdf in [
            pd.DataFrame(np.random.randn(10, 5), index=np.random.randint(100, size=10)),
            pd.DataFrame(
                np.random.randn(10, 5), index=np.random.randint(100, size=10).astype(np.int32)
            ),
            pd.DataFrame(np.random.randn(10, 5), index=np.random.randn(10)),
            pd.DataFrame(np.random.randn(10, 5), index=np.random.randn(10).astype(np.float32)),
            pd.DataFrame(np.random.randn(10, 5), index=list("abcdefghij")),
            pd.DataFrame(
                np.random.randn(10, 5), index=pd.date_range("2011-01-01", freq="D", periods=10)
            ),
            pd.DataFrame(np.random.randn(10, 5), index=pd.Categorical(list("abcdefghij"))),
            pd.DataFrame(np.random.randn(10, 5), columns=list("abcde")).set_index(["a", "b"]),
        ]:
            psdf = ps.from_pandas(pdf)
            self.assert_eq(psdf.index, pdf.index)
            self.assert_eq(psdf.index.dtype, pdf.index.dtype)

        self.assert_eq(ps.Index([])._summary(), "Index: 0 entries")
        with self.assertRaisesRegex(ValueError, "The truth value of a Index is ambiguous."):
            bool(ps.Index([1]))
        with self.assertRaisesRegex(TypeError, "Index.name must be a hashable type"):
            ps.Index([1, 2, 3], name=[(1, 2, 3)])
        with self.assertRaisesRegex(TypeError, "Index.name must be a hashable type"):
            ps.Index([1.0, 2.0, 3.0], name=[(1, 2, 3)])

    def test_index_getattr(self):
        psidx = self.psdf.index
        item = "databricks"

        expected_error_message = "'.*Index' object has no attribute '{}'".format(item)
        with self.assertRaisesRegex(AttributeError, expected_error_message):
            psidx.__getattr__(item)
        with self.assertRaisesRegex(AttributeError, expected_error_message):
            ps.from_pandas(pd.date_range("2011-01-01", freq="D", periods=10)).__getattr__(item)

    def test_multi_index_getattr(self):
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        idx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame(np.random.randn(4, 5), idx)
        psdf = ps.from_pandas(pdf)
        psidx = psdf.index
        item = "databricks"

        expected_error_message = "'MultiIndex' object has no attribute '{}'".format(item)
        with self.assertRaisesRegex(AttributeError, expected_error_message):
            psidx.__getattr__(item)

    def test_index_names(self):
        psdf = self.psdf
        self.assertIsNone(psdf.index.name)

        idx = pd.Index([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], name="x")
        pdf = pd.DataFrame(np.random.randn(10, 5), index=idx, columns=list("abcde"))
        psdf = ps.from_pandas(pdf)

        pser = pdf.a
        psser = psdf.a

        self.assertEqual(psdf.index.name, pdf.index.name)
        self.assertEqual(psdf.index.names, pdf.index.names)

        pidx = pdf.index
        psidx = psdf.index
        pidx.name = "renamed"
        psidx.name = "renamed"
        self.assertEqual(psidx.name, pidx.name)
        self.assertEqual(psidx.names, pidx.names)
        self.assert_eq(psidx, pidx)
        self.assertEqual(psdf.index.name, pdf.index.name)
        self.assertEqual(psdf.index.names, pdf.index.names)
        self.assertEqual(psser.index.names, pser.index.names)

        pidx.name = None
        psidx.name = None
        self.assertEqual(psidx.name, pidx.name)
        self.assertEqual(psidx.names, pidx.names)
        self.assert_eq(psidx, pidx)
        self.assertEqual(psdf.index.name, pdf.index.name)
        self.assertEqual(psdf.index.names, pdf.index.names)
        self.assertEqual(psser.index.names, pser.index.names)

        with self.assertRaisesRegex(ValueError, "Names must be a list-like"):
            psidx.names = "hi"

        expected_error_message = "Length of new names must be {}, got {}".format(
            psdf._internal.index_level, len(["0", "1"])
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            psidx.names = ["0", "1"]

        expected_error_message = "Index.name must be a hashable type"
        with self.assertRaisesRegex(TypeError, expected_error_message):
            ps.Index([1, 2, 3], name=["0", "1"])
        with self.assertRaisesRegex(TypeError, expected_error_message):
            psidx.name = ["renamed"]
        with self.assertRaisesRegex(TypeError, expected_error_message):
            psidx.name = ["0", "1"]
        # Specifying `names` when creating Index is no longer supported from pandas 2.0.0.
        if LooseVersion(pd.__version__) >= LooseVersion("2.0.0"):
            pass
        else:
            with self.assertRaisesRegex(TypeError, expected_error_message):
                ps.Index([(1, 2), (3, 4)], names=["a", ["b"]])

    def test_multi_index_names(self):
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        idx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame(np.random.randn(4, 5), idx)
        psdf = ps.from_pandas(pdf)

        self.assertEqual(psdf.index.names, pdf.index.names)

        pidx = pdf.index
        psidx = psdf.index
        pidx.names = ["renamed_number", "renamed_color"]
        psidx.names = ["renamed_number", "renamed_color"]
        self.assertEqual(psidx.names, pidx.names)

        pidx.names = ["renamed_number", None]
        psidx.names = ["renamed_number", None]
        self.assertEqual(psidx.names, pidx.names)
        self.assert_eq(psidx, pidx)

        with self.assertRaises(PandasNotImplementedError):
            psidx.name
        with self.assertRaises(PandasNotImplementedError):
            psidx.name = "renamed"

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

    def test_multi_index_levshape(self):
        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2)])
        psidx = ps.from_pandas(pidx)
        self.assertEqual(pidx.levshape, psidx.levshape)

    def test_multi_index_copy(self):
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        idx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame(np.random.randn(4, 5), idx)
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.index.copy(), pdf.index.copy())

    def test_missing(self):
        psdf = ps.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
                "c": pd.date_range("2011-01-01", freq="D", periods=3),
                "d": pd.Categorical(["a", "b", "c"]),
                "e": [timedelta(1), timedelta(2), timedelta(3)],
            }
        )

        # Index functions
        missing_functions = inspect.getmembers(MissingPandasLikeIndex, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index("a").index, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.set_index("a").index, name)()

        # MultiIndex functions
        missing_functions = inspect.getmembers(MissingPandasLikeMultiIndex, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index(["a", "b"]).index, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.set_index(["a", "b"]).index, name)()

        # DatetimeIndex functions
        missing_functions = inspect.getmembers(MissingPandasLikeDatetimeIndex, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index("c").index, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.set_index("c").index, name)()

        # TimedeltaIndex functions
        missing_functions = inspect.getmembers(MissingPandasLikeTimedeltaIndex, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index("e").index, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.set_index("e").index, name)()

        # Index properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeIndex, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index("a").index, name)

        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.set_index("a").index, name)

        # MultiIndex properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeMultiIndex, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index(["a", "b"]).index, name)

        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.set_index(["a", "b"]).index, name)

        # DatetimeIndex properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeDatetimeIndex, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index("c").index, name)

        # TimedeltaIndex properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeDatetimeIndex, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index("c").index, name)

    def test_multi_index_not_supported(self):
        psdf = ps.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})

        with self.assertRaisesRegex(TypeError, "cannot perform any with this index type"):
            psdf.set_index(["a", "b"]).index.any()

        with self.assertRaisesRegex(TypeError, "cannot perform all with this index type"):
            psdf.set_index(["a", "b"]).index.all()

    def test_index_nlevels(self):
        pdf = pd.DataFrame({"a": [1, 2, 3]}, index=pd.Index(["a", "b", "c"]))
        psdf = ps.from_pandas(pdf)

        self.assertEqual(psdf.index.nlevels, 1)

    def test_multiindex_nlevel(self):
        pdf = pd.DataFrame({"a": [1, 2, 3]}, index=[list("abc"), list("def")])
        psdf = ps.from_pandas(pdf)

        self.assertEqual(psdf.index.nlevels, 2)

    def test_multiindex_swaplevel(self):
        pidx = pd.MultiIndex.from_arrays([["a", "b"], [1, 2]])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.swaplevel(0, 1), psidx.swaplevel(0, 1))

        pidx = pd.MultiIndex.from_arrays([["a", "b"], [1, 2]], names=["word", "number"])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.swaplevel(0, 1), psidx.swaplevel(0, 1))

        pidx = pd.MultiIndex.from_arrays([["a", "b"], [1, 2]], names=["word", None])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.swaplevel(-2, -1), psidx.swaplevel(-2, -1))
        self.assert_eq(pidx.swaplevel(0, 1), psidx.swaplevel(0, 1))
        self.assert_eq(pidx.swaplevel("word", 1), psidx.swaplevel("word", 1))

        with self.assertRaisesRegex(IndexError, "Too many levels: Index"):
            psidx.swaplevel(-3, "word")
        with self.assertRaisesRegex(IndexError, "Too many levels: Index"):
            psidx.swaplevel(0, 2)
        with self.assertRaisesRegex(IndexError, "Too many levels: Index"):
            psidx.swaplevel(0, -3)
        with self.assertRaisesRegex(KeyError, "Level work not found"):
            psidx.swaplevel(0, "work")

    def test_index_fillna(self):
        pidx = pd.Index([1, 2, None])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.fillna(0), psidx.fillna(0), almost=True)
        self.assert_eq(pidx.rename("name").fillna(0), psidx.rename("name").fillna(0), almost=True)

        with self.assertRaisesRegex(TypeError, "Unsupported type list"):
            psidx.fillna([1, 2])

    def test_multiindex_isna(self):
        psidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])

        with self.assertRaisesRegex(NotImplementedError, "isna is not defined for MultiIndex"):
            psidx.isna()

        with self.assertRaisesRegex(NotImplementedError, "isna is not defined for MultiIndex"):
            psidx.isnull()

        with self.assertRaisesRegex(NotImplementedError, "notna is not defined for MultiIndex"):
            psidx.notna()

        with self.assertRaisesRegex(NotImplementedError, "notna is not defined for MultiIndex"):
            psidx.notnull()

    def test_multiindex_rename(self):
        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        psidx = ps.from_pandas(pidx)

        pidx = pidx.rename(list("ABC"))
        psidx = psidx.rename(list("ABC"))
        self.assert_eq(pidx, psidx)

        pidx = pidx.rename(["my", "name", "is"])
        psidx = psidx.rename(["my", "name", "is"])
        self.assert_eq(pidx, psidx)

    def test_multiindex_set_names(self):
        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        psidx = ps.from_pandas(pidx)

        pidx = pidx.set_names(["set", "new", "names"])
        psidx = psidx.set_names(["set", "new", "names"])
        self.assert_eq(pidx, psidx)

        pidx.set_names(["set", "new", "names"], inplace=True)
        psidx.set_names(["set", "new", "names"], inplace=True)
        self.assert_eq(pidx, psidx)

        pidx = pidx.set_names("first", level=0)
        psidx = psidx.set_names("first", level=0)
        self.assert_eq(pidx, psidx)

        pidx = pidx.set_names("second", level=1)
        psidx = psidx.set_names("second", level=1)
        self.assert_eq(pidx, psidx)

        pidx = pidx.set_names("third", level=2)
        psidx = psidx.set_names("third", level=2)
        self.assert_eq(pidx, psidx)

        pidx.set_names("first", level=0, inplace=True)
        psidx.set_names("first", level=0, inplace=True)
        self.assert_eq(pidx, psidx)

        pidx.set_names("second", level=1, inplace=True)
        psidx.set_names("second", level=1, inplace=True)
        self.assert_eq(pidx, psidx)

        pidx.set_names("third", level=2, inplace=True)
        psidx.set_names("third", level=2, inplace=True)
        self.assert_eq(pidx, psidx)

    def test_multiindex_tuple_column_name(self):
        column_labels = pd.MultiIndex.from_tuples([("a", "x"), ("a", "y"), ("b", "z")])
        pdf = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=column_labels)
        pdf.set_index(("a", "x"), append=True, inplace=True)
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf, psdf)

    def test_len(self):
        pidx = pd.Index(range(10000))
        psidx = ps.from_pandas(pidx)

        self.assert_eq(len(pidx), len(psidx))

        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        psidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])

        self.assert_eq(len(pidx), len(psidx))

    def test_argmin(self):
        pidx = pd.Index([100, 50, 10, 20, 30, 60, 0, 50, 0, 100, 100, 100, 20, 0, 0])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.argmin(), psidx.argmin())

        # MultiIndex
        psidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        with self.assertRaisesRegex(
            TypeError, "reduction operation 'argmin' not allowed for this dtype"
        ):
            psidx.argmin()

    def test_argmax(self):
        pidx = pd.Index([100, 50, 10, 20, 30, 60, 0, 50, 0, 100, 100, 100, 20, 0, 0])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.argmax(), psidx.argmax())

        # MultiIndex
        psidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        with self.assertRaisesRegex(
            TypeError, "reduction operation 'argmax' not allowed for this dtype"
        ):
            psidx.argmax()

    def test_min(self):
        pidx = pd.Index([3, 2, 1])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.min(), psidx.min())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2)])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.min(), psmidx.min())

        pidx = pd.DatetimeIndex(["2021-02-01", "2021-01-01", "2021-04-01", "2021-03-01"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.min(), psidx.min())

    def test_max(self):
        pidx = pd.Index([3, 2, 1])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.max(), psidx.max())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2)])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.max(), psmidx.max())

        pidx = pd.DatetimeIndex(["2021-02-01", "2021-01-01", "2021-04-01", "2021-03-01"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.max(), psidx.max())

    def test_repeat(self):
        pidx = pd.Index(["a", "b", "c"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.repeat(3).sort_values(), pidx.repeat(3).sort_values())
        self.assert_eq(psidx.repeat(0).sort_values(), pidx.repeat(0).sort_values())
        self.assert_eq((psidx + "x").repeat(3).sort_values(), (pidx + "x").repeat(3).sort_values())

        self.assertRaises(ValueError, lambda: psidx.repeat(-1))
        self.assertRaises(TypeError, lambda: psidx.repeat("abc"))

        pmidx = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(psmidx.repeat(3).sort_values(), pmidx.repeat(3).sort_values())
        self.assert_eq(psmidx.repeat(0).sort_values(), pmidx.repeat(0).sort_values(), almost=True)

        self.assertRaises(ValueError, lambda: psmidx.repeat(-1))
        self.assertRaises(TypeError, lambda: psmidx.repeat("abc"))

    def test_index_get_level_values(self):
        pidx = pd.Index([1, 2, 3], name="ks")
        psidx = ps.from_pandas(pidx)

        for level in [0, "ks"]:
            self.assert_eq(psidx.get_level_values(level), pidx.get_level_values(level))

    def test_multiindex_get_level_values(self):
        pmidx = pd.MultiIndex.from_tuples([("a", "d"), ("b", "e"), ("c", "f")])
        pmidx.names = ["level_1", "level_2"]
        psmidx = ps.from_pandas(pmidx)

        for level in [0, 1, "level_1", "level_2"]:
            self.assert_eq(psmidx.get_level_values(level), pmidx.get_level_values(level))

    def test_index_get_level_number(self):
        # name of two levels are the same, which is None
        psdf = ps.DataFrame({"a": [1, 2, 3]}, index=[list("aac"), list("ddf")])
        with self.assertRaisesRegex(
            ValueError, "The name None occurs multiple times, use a level number"
        ):
            psdf.index._get_level_number(None)

        mi = pd.MultiIndex.from_arrays((list("abc"), list("def")))
        mi.names = ["level_1", "level_2"]
        psdf = ps.DataFrame({"a": [1, 2, 3]}, index=mi)

        # level is not int and not in the level name list
        with self.assertRaisesRegex(KeyError, "Level lv_3 not found"):
            psdf.index._get_level_number("lv_3")

        # level is int, but an invalid negative number
        with self.assertRaisesRegex(IndexError, "Too many levels: Index has only"):
            psdf.index._get_level_number(-3)

        # level is int, but an invalid positive number
        with self.assertRaisesRegex(IndexError, "Too many levels: Index has only"):
            psdf.index._get_level_number(3)

        # Correct and valid inputs in numbers
        level_number = [-2, -1, 0, 1]
        outputs = [0, 1, 0, 1]

        for lv, output in zip(level_number, outputs):
            self.assertEqual(output, psdf.index._get_level_number(lv))

        # Valid inputs as level names
        level_names = ["level_1", "level_2"]
        outputs = [0, 1]

        for lv, output in zip(level_names, outputs):
            self.assertEqual(output, psdf.index._get_level_number(lv))

    def test_holds_integer(self):
        pidx = pd.Index([1, 2, 3, 4])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.holds_integer(), psidx.holds_integer())

        pidx = pd.Index([1.1, 2.2, 3.3, 4.4])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.holds_integer(), psidx.holds_integer())

        pidx = pd.Index(["A", "B", "C", "D"])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.holds_integer(), psidx.holds_integer())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "a")])
        psmidx = ps.from_pandas(pmidx)
        self.assert_eq(pmidx.holds_integer(), psmidx.holds_integer())

        pmidx = pd.MultiIndex.from_tuples([(10, 1), (10, 2), (20, 1)])
        psmidx = ps.from_pandas(pmidx)
        self.assert_eq(pmidx.holds_integer(), psmidx.holds_integer())

    def test_abs(self):
        pidx = pd.Index([-2, -1, 0, 1])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(abs(pidx), abs(psidx))
        self.assert_eq(np.abs(pidx), np.abs(psidx))

        psidx = ps.MultiIndex.from_tuples([(1, 2)], names=["level1", "level2"])
        with self.assertRaisesRegex(TypeError, "perform __abs__ with this index"):
            abs(psidx)

    def test_hasnans(self):
        # BooleanType
        pidx = pd.Index([True, False, True, True])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.hasnans, psidx.hasnans)

        pidx = pd.Index([True, False, np.nan, True])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.hasnans, psidx.hasnans)

        # TimestampType
        pser = pd.Series([pd.Timestamp("2020-07-30") for _ in range(3)])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.hasnans, psser.hasnans)

        pser = pd.Series([pd.Timestamp("2020-07-30"), np.nan, pd.Timestamp("2020-07-30")])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.hasnans, psser.hasnans)

        # empty
        pidx = pd.Index([])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.hasnans, psidx.hasnans)

        # Not supported for MultiIndex
        psmidx = ps.Index([("a", 1), ("b", 2)])
        self.assertRaises(NotImplementedError, lambda: psmidx.hasnans())

    def test_item(self):
        pidx = pd.Index([10])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.item(), psidx.item())

        # with timestamp
        pidx = pd.Index([datetime(1990, 3, 9)])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.item(), psidx.item())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x")])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.item(), psmidx.item())

        # MultiIndex with timestamp
        pmidx = pd.MultiIndex.from_tuples([(datetime(1990, 3, 9), datetime(2019, 8, 15))])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(pidx.item(), psidx.item())

        err_msg = "can only convert an array of size 1 to a Python scalar"
        with self.assertRaisesRegex(ValueError, err_msg):
            ps.Index([10, 20]).item()
        with self.assertRaisesRegex(ValueError, err_msg):
            ps.MultiIndex.from_tuples([("a", "x"), ("b", "y")]).item()

    def test_inferred_type(self):
        # Integer
        pidx = pd.Index([1, 2, 3])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.inferred_type, psidx.inferred_type)

        # Floating
        pidx = pd.Index([1.0, 2.0, 3.0])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.inferred_type, psidx.inferred_type)

        # String
        pidx = pd.Index(["a", "b", "c"])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.inferred_type, psidx.inferred_type)

        # Boolean
        pidx = pd.Index([True, False, True, False])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.inferred_type, psidx.inferred_type)

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x")])
        psmidx = ps.from_pandas(pmidx)
        self.assert_eq(pmidx.inferred_type, psmidx.inferred_type)

    def test_view(self):
        pidx = pd.Index([1, 2, 3, 4], name="Koalas")
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.view(), psidx.view())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.view(), psmidx.view())

    def test_index_ops(self):
        pidx = pd.Index([1, 2, 3, 4, 5])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx * 100 + psidx * 10 + psidx, pidx * 100 + pidx * 10 + pidx)

        pidx = pd.Index([1, 2, 3, 4, 5], name="a")
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx * 100 + psidx * 10 + psidx, pidx * 100 + pidx * 10 + pidx)

        pdf = pd.DataFrame(
            index=pd.MultiIndex.from_tuples([(1, 2), (3, 4), (5, 6)], names=["a", "b"])
        )
        psdf = ps.from_pandas(pdf)

        pidx1 = pdf.index.get_level_values(0)
        pidx2 = pdf.index.get_level_values(1)
        psidx1 = psdf.index.get_level_values(0)
        psidx2 = psdf.index.get_level_values(1)

        self.assert_eq(psidx1 * 10 + psidx2, pidx1 * 10 + pidx2)

    def test_factorize(self):
        pidx = pd.Index(["a", "b", "a", "b"])
        psidx = ps.from_pandas(pidx)
        pcodes, puniques = pidx.factorize(sort=True)
        kcodes, kuniques = psidx.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pmidx = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        psmidx = ps.from_pandas(pmidx)

        self.assertRaises(PandasNotImplementedError, lambda: psmidx.factorize())

    def test_multiindex_equal_levels(self):
        pmidx1 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        pmidx2 = pd.MultiIndex.from_tuples([("b", "y"), ("a", "x"), ("c", "z")])
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

        pmidx2 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "j")])
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

        pmidx2 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("a", "x")])
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

        pmidx2 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y")])
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

        pmidx2 = pd.MultiIndex.from_tuples([("a", "y"), ("b", "x"), ("c", "z")])
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

        pmidx1 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z"), ("a", "y")])
        pmidx2 = pd.MultiIndex.from_tuples([("a", "y"), ("b", "x"), ("c", "z"), ("c", "x")])
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

        pmidx1 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        pmidx2 = pd.MultiIndex.from_tuples([("a", "x", "q"), ("b", "y", "w"), ("c", "z", "e")])
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))


class IndexesTests(
    IndexesTestsMixin,
    ComparisonTestBase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_base import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
