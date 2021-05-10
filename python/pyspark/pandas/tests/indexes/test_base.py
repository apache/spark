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
from distutils.version import LooseVersion
from datetime import datetime

import numpy as np
import pandas as pd

import pyspark.pandas as ps
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.pandas.missing.indexes import (
    MissingPandasLikeCategoricalIndex,
    MissingPandasLikeDatetimeIndex,
    MissingPandasLikeIndex,
    MissingPandasLikeMultiIndex,
)
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils, SPARK_CONF_ARROW_ENABLED


class IndexesTest(PandasOnSparkTestCase, TestUtils):
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )

    @property
    def kdf(self):
        return ps.from_pandas(self.pdf)

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
            kdf = ps.from_pandas(pdf)
            self.assert_eq(kdf.index, pdf.index)
            self.assert_eq(type(kdf.index).__name__, type(pdf.index).__name__)

    def test_index_from_series(self):
        pser = pd.Series([1, 2, 3], name="a", index=[10, 20, 30])
        kser = ps.from_pandas(pser)

        self.assert_eq(ps.Index(kser), pd.Index(pser))
        self.assert_eq(ps.Index(kser, dtype="float"), pd.Index(pser, dtype="float"))
        self.assert_eq(ps.Index(kser, name="x"), pd.Index(pser, name="x"))

        if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
            self.assert_eq(ps.Int64Index(kser), pd.Int64Index(pser))
            self.assert_eq(ps.Float64Index(kser), pd.Float64Index(pser))
        else:
            self.assert_eq(ps.Int64Index(kser), pd.Int64Index(pser).rename("a"))
            self.assert_eq(ps.Float64Index(kser), pd.Float64Index(pser).rename("a"))

        pser = pd.Series([datetime(2021, 3, 1), datetime(2021, 3, 2)], name="x", index=[10, 20])
        kser = ps.from_pandas(pser)

        self.assert_eq(ps.Index(kser), pd.Index(pser))
        self.assert_eq(ps.DatetimeIndex(kser), pd.DatetimeIndex(pser))

    def test_index_from_index(self):
        pidx = pd.Index([1, 2, 3], name="a")
        kidx = ps.from_pandas(pidx)

        self.assert_eq(ps.Index(kidx), pd.Index(pidx))
        self.assert_eq(ps.Index(kidx, dtype="float"), pd.Index(pidx, dtype="float"))
        self.assert_eq(ps.Index(kidx, name="x"), pd.Index(pidx, name="x"))

        self.assert_eq(ps.Int64Index(kidx), pd.Int64Index(pidx))
        self.assert_eq(ps.Float64Index(kidx), pd.Float64Index(pidx))

        pidx = pd.DatetimeIndex(["2021-03-01", "2021-03-02"])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(ps.Index(kidx), pd.Index(pidx))
        self.assert_eq(ps.DatetimeIndex(kidx), pd.DatetimeIndex(pidx))

    def test_index_getattr(self):
        kidx = self.kdf.index
        item = "databricks"

        expected_error_message = "'.*Index' object has no attribute '{}'".format(item)
        with self.assertRaisesRegex(AttributeError, expected_error_message):
            kidx.__getattr__(item)
        with self.assertRaisesRegex(AttributeError, expected_error_message):
            ps.from_pandas(pd.date_range("2011-01-01", freq="D", periods=10)).__getattr__(item)

    def test_multi_index_getattr(self):
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        idx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame(np.random.randn(4, 5), idx)
        kdf = ps.from_pandas(pdf)
        kidx = kdf.index
        item = "databricks"

        expected_error_message = "'MultiIndex' object has no attribute '{}'".format(item)
        with self.assertRaisesRegex(AttributeError, expected_error_message):
            kidx.__getattr__(item)

    def test_to_series(self):
        pidx = self.pdf.index
        kidx = self.kdf.index

        self.assert_eq(kidx.to_series(), pidx.to_series())
        self.assert_eq(kidx.to_series(name="a"), pidx.to_series(name="a"))

        # With name
        pidx.name = "Koalas"
        kidx.name = "Koalas"
        self.assert_eq(kidx.to_series(), pidx.to_series())
        self.assert_eq(kidx.to_series(name=("x", "a")), pidx.to_series(name=("x", "a")))

        # With tupled name
        pidx.name = ("x", "a")
        kidx.name = ("x", "a")
        self.assert_eq(kidx.to_series(), pidx.to_series())
        self.assert_eq(kidx.to_series(name="a"), pidx.to_series(name="a"))

        self.assert_eq((kidx + 1).to_series(), (pidx + 1).to_series())

        pidx = self.pdf.set_index("b", append=True).index
        kidx = self.kdf.set_index("b", append=True).index

        with self.sql_conf({SPARK_CONF_ARROW_ENABLED: False}):
            self.assert_eq(kidx.to_series(), pidx.to_series())
            self.assert_eq(kidx.to_series(name="a"), pidx.to_series(name="a"))

        expected_error_message = "Series.name must be a hashable type"
        with self.assertRaisesRegex(TypeError, expected_error_message):
            kidx.to_series(name=["x", "a"])

    def test_to_frame(self):
        pidx = self.pdf.index
        kidx = self.kdf.index

        self.assert_eq(kidx.to_frame(), pidx.to_frame())
        self.assert_eq(kidx.to_frame(index=False), pidx.to_frame(index=False))

        pidx.name = "a"
        kidx.name = "a"

        self.assert_eq(kidx.to_frame(), pidx.to_frame())
        self.assert_eq(kidx.to_frame(index=False), pidx.to_frame(index=False))

        if LooseVersion(pd.__version__) >= LooseVersion("0.24"):
            # The `name` argument is added in pandas 0.24.
            self.assert_eq(kidx.to_frame(name="x"), pidx.to_frame(name="x"))
            self.assert_eq(
                kidx.to_frame(index=False, name="x"), pidx.to_frame(index=False, name="x"),
            )

            self.assertRaises(TypeError, lambda: kidx.to_frame(name=["x"]))

            # non-string name
            self.assert_eq(kidx.to_frame(name=10), pidx.to_frame(name=10))
            self.assert_eq(kidx.to_frame(name=("x", 10)), pidx.to_frame(name=("x", 10)))

        pidx = self.pdf.set_index("b", append=True).index
        kidx = self.kdf.set_index("b", append=True).index

        self.assert_eq(kidx.to_frame(), pidx.to_frame())
        self.assert_eq(kidx.to_frame(index=False), pidx.to_frame(index=False))

        if LooseVersion(pd.__version__) >= LooseVersion("0.24"):
            # The `name` argument is added in pandas 0.24.
            self.assert_eq(kidx.to_frame(name=["x", "y"]), pidx.to_frame(name=["x", "y"]))
            self.assert_eq(kidx.to_frame(name=("x", "y")), pidx.to_frame(name=("x", "y")))
            self.assert_eq(
                kidx.to_frame(index=False, name=["x", "y"]),
                pidx.to_frame(index=False, name=["x", "y"]),
            )

            self.assertRaises(TypeError, lambda: kidx.to_frame(name="x"))
            self.assertRaises(ValueError, lambda: kidx.to_frame(name=["x"]))

            # non-string names
            self.assert_eq(kidx.to_frame(name=[10, 20]), pidx.to_frame(name=[10, 20]))
            self.assert_eq(kidx.to_frame(name=("x", 10)), pidx.to_frame(name=("x", 10)))
            self.assert_eq(
                kidx.to_frame(name=[("x", 10), ("y", 20)]),
                pidx.to_frame(name=[("x", 10), ("y", 20)]),
            )

    def test_index_names(self):
        kdf = self.kdf
        self.assertIsNone(kdf.index.name)

        idx = pd.Index([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], name="x")
        pdf = pd.DataFrame(np.random.randn(10, 5), index=idx, columns=list("abcde"))
        kdf = ps.from_pandas(pdf)

        pser = pdf.a
        kser = kdf.a

        self.assertEqual(kdf.index.name, pdf.index.name)
        self.assertEqual(kdf.index.names, pdf.index.names)

        pidx = pdf.index
        kidx = kdf.index
        pidx.name = "renamed"
        kidx.name = "renamed"
        self.assertEqual(kidx.name, pidx.name)
        self.assertEqual(kidx.names, pidx.names)
        self.assert_eq(kidx, pidx)
        self.assertEqual(kdf.index.name, pdf.index.name)
        self.assertEqual(kdf.index.names, pdf.index.names)
        self.assertEqual(kser.index.names, pser.index.names)

        pidx.name = None
        kidx.name = None
        self.assertEqual(kidx.name, pidx.name)
        self.assertEqual(kidx.names, pidx.names)
        self.assert_eq(kidx, pidx)
        self.assertEqual(kdf.index.name, pdf.index.name)
        self.assertEqual(kdf.index.names, pdf.index.names)
        self.assertEqual(kser.index.names, pser.index.names)

        with self.assertRaisesRegex(ValueError, "Names must be a list-like"):
            kidx.names = "hi"

        expected_error_message = "Length of new names must be {}, got {}".format(
            kdf._internal.index_level, len(["0", "1"])
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            kidx.names = ["0", "1"]

        expected_error_message = "Index.name must be a hashable type"
        with self.assertRaisesRegex(TypeError, expected_error_message):
            ps.Index([1, 2, 3], name=["0", "1"])
        with self.assertRaisesRegex(TypeError, expected_error_message):
            kidx.name = ["renamed"]
        with self.assertRaisesRegex(TypeError, expected_error_message):
            kidx.name = ["0", "1"]
        with self.assertRaisesRegex(TypeError, expected_error_message):
            ps.Index([(1, 2), (3, 4)], names=["a", ["b"]])

    def test_multi_index_names(self):
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        idx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame(np.random.randn(4, 5), idx)
        kdf = ps.from_pandas(pdf)

        self.assertEqual(kdf.index.names, pdf.index.names)

        pidx = pdf.index
        kidx = kdf.index
        pidx.names = ["renamed_number", "renamed_color"]
        kidx.names = ["renamed_number", "renamed_color"]
        self.assertEqual(kidx.names, pidx.names)

        pidx.names = ["renamed_number", None]
        kidx.names = ["renamed_number", None]
        self.assertEqual(kidx.names, pidx.names)
        self.assert_eq(kidx, pidx)

        with self.assertRaises(PandasNotImplementedError):
            kidx.name
        with self.assertRaises(PandasNotImplementedError):
            kidx.name = "renamed"

    def test_index_rename(self):
        pdf = pd.DataFrame(
            np.random.randn(10, 5), index=pd.Index([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], name="x")
        )
        kdf = ps.from_pandas(pdf)

        pidx = pdf.index
        kidx = kdf.index

        self.assert_eq(kidx.rename("y"), pidx.rename("y"))
        self.assert_eq(kdf.index.names, pdf.index.names)

        # non-string names
        self.assert_eq(kidx.rename(0), pidx.rename(0))
        self.assert_eq(kidx.rename(("y", 0)), pidx.rename(("y", 0)))

        kidx.rename("z", inplace=True)
        pidx.rename("z", inplace=True)

        self.assert_eq(kidx, pidx)
        self.assert_eq(kdf.index.names, pdf.index.names)

        self.assert_eq(kidx.rename(None), pidx.rename(None))
        self.assert_eq(kdf.index.names, pdf.index.names)

        self.assertRaises(TypeError, lambda: kidx.rename(["x", "y"]))

    def test_multi_index_rename(self):
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        idx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame(np.random.randn(4, 5), idx)
        kdf = ps.from_pandas(pdf)

        pmidx = pdf.index
        kmidx = kdf.index

        self.assert_eq(kmidx.rename(["n", "c"]), pmidx.rename(["n", "c"]))
        self.assert_eq(kdf.index.names, pdf.index.names)

        # non-string names
        self.assert_eq(kmidx.rename([0, 1]), pmidx.rename([0, 1]))
        self.assert_eq(
            kmidx.rename([("x", "a"), ("y", "b")]), pmidx.rename([("x", "a"), ("y", "b")])
        )

        kmidx.rename(["num", "col"], inplace=True)
        pmidx.rename(["num", "col"], inplace=True)

        self.assert_eq(kmidx, pmidx)
        self.assert_eq(kdf.index.names, pdf.index.names)

        self.assert_eq(kmidx.rename([None, None]), pmidx.rename([None, None]))
        self.assert_eq(kdf.index.names, pdf.index.names)

        self.assertRaises(TypeError, lambda: kmidx.rename("number"))
        self.assertRaises(TypeError, lambda: kmidx.rename(None))
        self.assertRaises(ValueError, lambda: kmidx.rename(["number"]))

    def test_multi_index_levshape(self):
        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2)])
        kidx = ps.from_pandas(pidx)
        self.assertEqual(pidx.levshape, kidx.levshape)

    def test_index_unique(self):
        kidx = self.kdf.index

        # here the output is different than pandas in terms of order
        expected = [0, 1, 3, 5, 6, 8, 9]

        self.assert_eq(expected, sorted(kidx.unique().to_pandas()))
        self.assert_eq(expected, sorted(kidx.unique(level=0).to_pandas()))

        expected = [1, 2, 4, 6, 7, 9, 10]
        self.assert_eq(expected, sorted((kidx + 1).unique().to_pandas()))

        with self.assertRaisesRegex(IndexError, "Too many levels*"):
            kidx.unique(level=1)

        with self.assertRaisesRegex(KeyError, "Requested level (hi)*"):
            kidx.unique(level="hi")

    def test_multi_index_copy(self):
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        idx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame(np.random.randn(4, 5), idx)
        kdf = ps.from_pandas(pdf)

        self.assert_eq(kdf.index.copy(), pdf.index.copy())

    def test_drop_duplicates(self):
        pidx = pd.Index([4, 2, 4, 1, 4, 3])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(kidx.drop_duplicates().sort_values(), pidx.drop_duplicates().sort_values())
        self.assert_eq(
            (kidx + 1).drop_duplicates().sort_values(), (pidx + 1).drop_duplicates().sort_values()
        )

    def test_dropna(self):
        pidx = pd.Index([np.nan, 2, 4, 1, np.nan, 3])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(kidx.dropna(), pidx.dropna())
        self.assert_eq((kidx + 1).dropna(), (pidx + 1).dropna())

    def test_index_symmetric_difference(self):
        pidx1 = pd.Index([1, 2, 3, 4])
        pidx2 = pd.Index([2, 3, 4, 5])
        kidx1 = ps.from_pandas(pidx1)
        kidx2 = ps.from_pandas(pidx2)

        self.assert_eq(
            kidx1.symmetric_difference(kidx2).sort_values(),
            pidx1.symmetric_difference(pidx2).sort_values(),
        )
        self.assert_eq(
            (kidx1 + 1).symmetric_difference(kidx2).sort_values(),
            (pidx1 + 1).symmetric_difference(pidx2).sort_values(),
        )

        pmidx1 = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 0, 0, 0, 1, 2, 0, 1, 2]],
        )
        pmidx2 = pd.MultiIndex(
            [["koalas", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 0, 0, 0, 1, 2, 0, 1, 2]],
        )
        kmidx1 = ps.from_pandas(pmidx1)
        kmidx2 = ps.from_pandas(pmidx2)

        self.assert_eq(
            kmidx1.symmetric_difference(kmidx2).sort_values(),
            pmidx1.symmetric_difference(pmidx2).sort_values(),
        )

        idx = ps.Index(["a", "b", "c"])
        midx = ps.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])

        with self.assertRaisesRegex(NotImplementedError, "Doesn't support*"):
            idx.symmetric_difference(midx)

    def test_multi_index_symmetric_difference(self):
        idx = ps.Index(["a", "b", "c"])
        midx = ps.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        midx_ = ps.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])

        self.assert_eq(
            midx.symmetric_difference(midx_),
            midx.to_pandas().symmetric_difference(midx_.to_pandas()),
        )

        with self.assertRaisesRegex(NotImplementedError, "Doesn't support*"):
            midx.symmetric_difference(idx)

    def test_missing(self):
        kdf = ps.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
                "c": pd.date_range("2011-01-01", freq="D", periods=3),
                "d": pd.Categorical(["a", "b", "c"]),
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
                getattr(kdf.set_index("a").index, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(kdf.set_index("a").index, name)()

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
                getattr(kdf.set_index(["a", "b"]).index, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(kdf.set_index(["a", "b"]).index, name)()

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
                getattr(kdf.set_index("c").index, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(kdf.set_index("c").index, name)()

        # CategoricalIndex functions
        missing_functions = inspect.getmembers(
            MissingPandasLikeCategoricalIndex, inspect.isfunction
        )
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(kdf.set_index("d").index, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(kdf.set_index("d").index, name)()

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
                getattr(kdf.set_index("a").index, name)

        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(kdf.set_index("a").index, name)

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
                getattr(kdf.set_index(["a", "b"]).index, name)

        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(kdf.set_index(["a", "b"]).index, name)

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
                getattr(kdf.set_index("c").index, name)

        # CategoricalIndex properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeCategoricalIndex, lambda o: isinstance(o, property)
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
                getattr(kdf.set_index("d").index, name)

    def test_index_has_duplicates(self):
        indexes = [("a", "b", "c"), ("a", "a", "c"), (1, 3, 3), (1, 2, 3)]
        names = [None, "ks", "ks", None]
        has_dup = [False, True, True, False]

        for idx, name, expected in zip(indexes, names, has_dup):
            pdf = pd.DataFrame({"a": [1, 2, 3]}, index=pd.Index(idx, name=name))
            kdf = ps.from_pandas(pdf)

            self.assertEqual(kdf.index.has_duplicates, expected)

    def test_multiindex_has_duplicates(self):
        indexes = [
            [list("abc"), list("edf")],
            [list("aac"), list("edf")],
            [list("aac"), list("eef")],
            [[1, 4, 4], [4, 6, 6]],
        ]
        has_dup = [False, False, True, True]

        for idx, expected in zip(indexes, has_dup):
            pdf = pd.DataFrame({"a": [1, 2, 3]}, index=idx)
            kdf = ps.from_pandas(pdf)

            self.assertEqual(kdf.index.has_duplicates, expected)

    def test_multi_index_not_supported(self):
        kdf = ps.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})

        with self.assertRaisesRegex(TypeError, "cannot perform any with this index type"):
            kdf.set_index(["a", "b"]).index.any()

        with self.assertRaisesRegex(TypeError, "cannot perform all with this index type"):
            kdf.set_index(["a", "b"]).index.all()

    def test_index_nlevels(self):
        pdf = pd.DataFrame({"a": [1, 2, 3]}, index=pd.Index(["a", "b", "c"]))
        kdf = ps.from_pandas(pdf)

        self.assertEqual(kdf.index.nlevels, 1)

    def test_multiindex_nlevel(self):
        pdf = pd.DataFrame({"a": [1, 2, 3]}, index=[list("abc"), list("def")])
        kdf = ps.from_pandas(pdf)

        self.assertEqual(kdf.index.nlevels, 2)

    def test_multiindex_from_arrays(self):
        arrays = [["a", "a", "b", "b"], ["red", "blue", "red", "blue"]]
        pidx = pd.MultiIndex.from_arrays(arrays)
        kidx = ps.MultiIndex.from_arrays(arrays)

        self.assert_eq(pidx, kidx)

    def test_multiindex_swaplevel(self):
        pidx = pd.MultiIndex.from_arrays([["a", "b"], [1, 2]])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.swaplevel(0, 1), kidx.swaplevel(0, 1))

        pidx = pd.MultiIndex.from_arrays([["a", "b"], [1, 2]], names=["word", "number"])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.swaplevel(0, 1), kidx.swaplevel(0, 1))

        pidx = pd.MultiIndex.from_arrays([["a", "b"], [1, 2]], names=["word", None])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.swaplevel(-2, -1), kidx.swaplevel(-2, -1))
        self.assert_eq(pidx.swaplevel(0, 1), kidx.swaplevel(0, 1))
        self.assert_eq(pidx.swaplevel("word", 1), kidx.swaplevel("word", 1))

        with self.assertRaisesRegex(IndexError, "Too many levels: Index"):
            kidx.swaplevel(-3, "word")
        with self.assertRaisesRegex(IndexError, "Too many levels: Index"):
            kidx.swaplevel(0, 2)
        with self.assertRaisesRegex(IndexError, "Too many levels: Index"):
            kidx.swaplevel(0, -3)
        with self.assertRaisesRegex(KeyError, "Level work not found"):
            kidx.swaplevel(0, "work")

    def test_multiindex_droplevel(self):
        pidx = pd.MultiIndex.from_tuples(
            [("a", "x", 1), ("b", "y", 2)], names=["level1", "level2", "level3"]
        )
        kidx = ps.from_pandas(pidx)
        with self.assertRaisesRegex(IndexError, "Too many levels: Index has only 3 levels, not 5"):
            kidx.droplevel(4)

        with self.assertRaisesRegex(KeyError, "Level level4 not found"):
            kidx.droplevel("level4")

        with self.assertRaisesRegex(KeyError, "Level.*level3.*level4.*not found"):
            kidx.droplevel([("level3", "level4")])

        with self.assertRaisesRegex(
            ValueError,
            "Cannot remove 4 levels from an index with 3 levels: at least one "
            "level must be left.",
        ):
            kidx.droplevel([0, 0, 1, 2])

        with self.assertRaisesRegex(
            ValueError,
            "Cannot remove 3 levels from an index with 3 levels: at least one "
            "level must be left.",
        ):
            kidx.droplevel([0, 1, 2])

        self.assert_eq(pidx.droplevel(0), kidx.droplevel(0))
        self.assert_eq(pidx.droplevel([0, 1]), kidx.droplevel([0, 1]))
        self.assert_eq(pidx.droplevel((0, 1)), kidx.droplevel((0, 1)))
        self.assert_eq(pidx.droplevel([0, "level2"]), kidx.droplevel([0, "level2"]))
        self.assert_eq(pidx.droplevel((0, "level2")), kidx.droplevel((0, "level2")))

        # non-string names
        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2)], names=[1.0, 2.0, 3.0])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.droplevel(1.0), kidx.droplevel(1.0))
        self.assert_eq(pidx.droplevel([0, 2.0]), kidx.droplevel([0, 2.0]))

    def test_index_fillna(self):
        pidx = pd.Index([1, 2, None])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.fillna(0), kidx.fillna(0), almost=True)
        self.assert_eq(pidx.rename("name").fillna(0), kidx.rename("name").fillna(0), almost=True)

        with self.assertRaisesRegex(TypeError, "Unsupported type list"):
            kidx.fillna([1, 2])

    def test_index_drop(self):
        pidx = pd.Index([1, 2, 3])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.drop(1), kidx.drop(1))
        self.assert_eq(pidx.drop([1, 2]), kidx.drop([1, 2]))
        self.assert_eq((pidx + 1).drop([2, 3]), (kidx + 1).drop([2, 3]))

    def test_multiindex_drop(self):
        pidx = pd.MultiIndex.from_tuples(
            [("a", "x"), ("b", "y"), ("c", "z")], names=["level1", "level2"]
        )
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.drop("a"), kidx.drop("a"))
        self.assert_eq(pidx.drop(["a", "b"]), kidx.drop(["a", "b"]))
        self.assert_eq(pidx.drop(["x", "y"], level=1), kidx.drop(["x", "y"], level=1))
        self.assert_eq(pidx.drop(["x", "y"], level="level2"), kidx.drop(["x", "y"], level="level2"))

        pidx.names = ["lv1", "lv2"]
        kidx.names = ["lv1", "lv2"]
        self.assert_eq(pidx.drop(["x", "y"], level="lv2"), kidx.drop(["x", "y"], level="lv2"))

        self.assertRaises(IndexError, lambda: kidx.drop(["a", "b"], level=2))
        self.assertRaises(KeyError, lambda: kidx.drop(["a", "b"], level="level"))

        kidx.names = ["lv", "lv"]
        self.assertRaises(ValueError, lambda: kidx.drop(["x", "y"], level="lv"))

    def test_sort_values(self):
        pidx = pd.Index([-10, -100, 200, 100])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.sort_values(), kidx.sort_values())
        self.assert_eq(pidx.sort_values(ascending=False), kidx.sort_values(ascending=False))

        pidx.name = "koalas"
        kidx.name = "koalas"

        self.assert_eq(pidx.sort_values(), kidx.sort_values())
        self.assert_eq(pidx.sort_values(ascending=False), kidx.sort_values(ascending=False))

        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        kidx = ps.from_pandas(pidx)

        pidx.names = ["hello", "koalas", "goodbye"]
        kidx.names = ["hello", "koalas", "goodbye"]

        self.assert_eq(pidx.sort_values(), kidx.sort_values())
        self.assert_eq(pidx.sort_values(ascending=False), kidx.sort_values(ascending=False))

    def test_index_drop_duplicates(self):
        pidx = pd.Index([1, 1, 2])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.drop_duplicates().sort_values(), kidx.drop_duplicates().sort_values())

        pidx = pd.MultiIndex.from_tuples([(1, 1), (1, 1), (2, 2)], names=["level1", "level2"])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.drop_duplicates().sort_values(), kidx.drop_duplicates().sort_values())

    def test_index_sort(self):
        idx = ps.Index([1, 2, 3, 4, 5])
        midx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2)])

        with self.assertRaisesRegex(
            TypeError, "cannot sort an Index object in-place, use sort_values instead"
        ):
            idx.sort()
        with self.assertRaisesRegex(
            TypeError, "cannot sort an Index object in-place, use sort_values instead"
        ):
            midx.sort()

    def test_multiindex_isna(self):
        kidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])

        with self.assertRaisesRegex(NotImplementedError, "isna is not defined for MultiIndex"):
            kidx.isna()

        with self.assertRaisesRegex(NotImplementedError, "isna is not defined for MultiIndex"):
            kidx.isnull()

        with self.assertRaisesRegex(NotImplementedError, "notna is not defined for MultiIndex"):
            kidx.notna()

        with self.assertRaisesRegex(NotImplementedError, "notna is not defined for MultiIndex"):
            kidx.notnull()

    def test_index_nunique(self):
        pidx = pd.Index([1, 1, 2, None])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.nunique(), kidx.nunique())
        self.assert_eq(pidx.nunique(dropna=True), kidx.nunique(dropna=True))

    def test_multiindex_nunique(self):
        kidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        with self.assertRaisesRegex(NotImplementedError, "notna is not defined for MultiIndex"):
            kidx.notnull()

    def test_multiindex_rename(self):
        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        kidx = ps.from_pandas(pidx)

        pidx = pidx.rename(list("ABC"))
        kidx = kidx.rename(list("ABC"))
        self.assert_eq(pidx, kidx)

        pidx = pidx.rename(["my", "name", "is"])
        kidx = kidx.rename(["my", "name", "is"])
        self.assert_eq(pidx, kidx)

    def test_multiindex_set_names(self):
        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        kidx = ps.from_pandas(pidx)

        pidx = pidx.set_names(["set", "new", "names"])
        kidx = kidx.set_names(["set", "new", "names"])
        self.assert_eq(pidx, kidx)

        pidx.set_names(["set", "new", "names"], inplace=True)
        kidx.set_names(["set", "new", "names"], inplace=True)
        self.assert_eq(pidx, kidx)

        pidx = pidx.set_names("first", level=0)
        kidx = kidx.set_names("first", level=0)
        self.assert_eq(pidx, kidx)

        pidx = pidx.set_names("second", level=1)
        kidx = kidx.set_names("second", level=1)
        self.assert_eq(pidx, kidx)

        pidx = pidx.set_names("third", level=2)
        kidx = kidx.set_names("third", level=2)
        self.assert_eq(pidx, kidx)

        pidx.set_names("first", level=0, inplace=True)
        kidx.set_names("first", level=0, inplace=True)
        self.assert_eq(pidx, kidx)

        pidx.set_names("second", level=1, inplace=True)
        kidx.set_names("second", level=1, inplace=True)
        self.assert_eq(pidx, kidx)

        pidx.set_names("third", level=2, inplace=True)
        kidx.set_names("third", level=2, inplace=True)
        self.assert_eq(pidx, kidx)

    def test_multiindex_from_tuples(self):
        tuples = [(1, "red"), (1, "blue"), (2, "red"), (2, "blue")]
        pidx = pd.MultiIndex.from_tuples(tuples)
        kidx = ps.MultiIndex.from_tuples(tuples)

        self.assert_eq(pidx, kidx)

    def test_multiindex_from_product(self):
        iterables = [[0, 1, 2], ["green", "purple"]]
        pidx = pd.MultiIndex.from_product(iterables)
        kidx = ps.MultiIndex.from_product(iterables)

        self.assert_eq(pidx, kidx)

    def test_multiindex_tuple_column_name(self):
        column_labels = pd.MultiIndex.from_tuples([("a", "x"), ("a", "y"), ("b", "z")])
        pdf = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=column_labels)
        pdf.set_index(("a", "x"), append=True, inplace=True)
        kdf = ps.from_pandas(pdf)
        self.assert_eq(pdf, kdf)

    def test_len(self):
        pidx = pd.Index(range(10000))
        kidx = ps.from_pandas(pidx)

        self.assert_eq(len(pidx), len(kidx))

        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        kidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])

        self.assert_eq(len(pidx), len(kidx))

    def test_delete(self):
        pidx = pd.Index([10, 9, 8, 7, 6, 7, 8, 9, 10])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.delete(8).sort_values(), kidx.delete(8).sort_values())
        self.assert_eq(pidx.delete(-9).sort_values(), kidx.delete(-9).sort_values())
        self.assert_eq(pidx.delete([-9, 0, 8]).sort_values(), kidx.delete([-9, 0, 8]).sort_values())

        with self.assertRaisesRegex(IndexError, "index 9 is out of bounds for axis 0 with size 9"):
            kidx.delete([0, 9])
        with self.assertRaisesRegex(
            IndexError, "index -10 is out of bounds for axis 0 with size 9"
        ):
            kidx.delete([-10, 0])
        with self.assertRaisesRegex(IndexError, "index 9 is out of bounds for axis 0 with size 9"):
            kidx.delete(9)
        with self.assertRaisesRegex(
            IndexError, "index -10 is out of bounds for axis 0 with size 9"
        ):
            kidx.delete(-10)

        # MultiIndex
        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        kidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])

        self.assert_eq(pidx.delete(2).sort_values(), kidx.delete(2).sort_values())
        self.assert_eq(pidx.delete(-3).sort_values(), kidx.delete(-3).sort_values())
        self.assert_eq(pidx.delete([-3, 0, 2]).sort_values(), kidx.delete([-3, 0, 2]).sort_values())

        with self.assertRaisesRegex(IndexError, "index 3 is out of bounds for axis 0 with size 3"):
            kidx.delete([0, 3])
        with self.assertRaisesRegex(IndexError, "index -4 is out of bounds for axis 0 with size 3"):
            kidx.delete([-4, 0])
        with self.assertRaisesRegex(IndexError, "index 3 is out of bounds for axis 0 with size 3"):
            kidx.delete(3)
        with self.assertRaisesRegex(IndexError, "index -4 is out of bounds for axis 0 with size 3"):
            kidx.delete(-4)

    def test_append(self):
        # Index
        pidx = pd.Index(range(10000))
        kidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.append(pidx), kidx.append(kidx))

        # Index with name
        pidx1 = pd.Index(range(10000), name="a")
        pidx2 = pd.Index(range(10000), name="b")
        kidx1 = ps.from_pandas(pidx1)
        kidx2 = ps.from_pandas(pidx2)

        self.assert_eq(pidx1.append(pidx2), kidx1.append(kidx2))

        self.assert_eq(pidx2.append(pidx1), kidx2.append(kidx1))

        # Index from DataFrame
        pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=["a", "b", "c"])
        pdf2 = pd.DataFrame({"a": [7, 8, 9], "d": [10, 11, 12]}, index=["x", "y", "z"])
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        pidx1 = pdf1.set_index("a").index
        pidx2 = pdf2.set_index("d").index
        kidx1 = kdf1.set_index("a").index
        kidx2 = kdf2.set_index("d").index

        self.assert_eq(pidx1.append(pidx2), kidx1.append(kidx2))

        self.assert_eq(pidx2.append(pidx1), kidx2.append(kidx1))

        # Index from DataFrame with MultiIndex columns
        pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        pdf2 = pd.DataFrame({"a": [7, 8, 9], "d": [10, 11, 12]})
        pdf1.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y")])
        pdf2.columns = pd.MultiIndex.from_tuples([("a", "x"), ("d", "y")])
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        pidx1 = pdf1.set_index(("a", "x")).index
        pidx2 = pdf2.set_index(("d", "y")).index
        kidx1 = kdf1.set_index(("a", "x")).index
        kidx2 = kdf2.set_index(("d", "y")).index

        self.assert_eq(pidx1.append(pidx2), kidx1.append(kidx2))

        self.assert_eq(pidx2.append(pidx1), kidx2.append(kidx1))

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        kmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.append(pmidx), kmidx.append(kmidx))

        # MultiIndex with names
        pmidx1 = pd.MultiIndex.from_tuples(
            [("a", "x", 1), ("b", "y", 2), ("c", "z", 3)], names=["x", "y", "z"]
        )
        pmidx2 = pd.MultiIndex.from_tuples(
            [("a", "x", 1), ("b", "y", 2), ("c", "z", 3)], names=["p", "q", "r"]
        )
        kmidx1 = ps.from_pandas(pmidx1)
        kmidx2 = ps.from_pandas(pmidx2)

        self.assert_eq(pmidx1.append(pmidx2), kmidx1.append(kmidx2))

        self.assert_eq(pmidx2.append(pmidx1), kmidx2.append(kmidx1))

        self.assert_eq(pmidx1.append(pmidx2).names, kmidx1.append(kmidx2).names)

        self.assert_eq(pmidx1.append(pmidx2).names, kmidx1.append(kmidx2).names)

        # Index & MultiIndex currently is not supported
        expected_error_message = r"append\(\) between Index & MultiIndex currently is not supported"
        with self.assertRaisesRegex(NotImplementedError, expected_error_message):
            kidx.append(kmidx)
        with self.assertRaisesRegex(NotImplementedError, expected_error_message):
            kmidx.append(kidx)

    def test_argmin(self):
        pidx = pd.Index([100, 50, 10, 20, 30, 60, 0, 50, 0, 100, 100, 100, 20, 0, 0])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.argmin(), kidx.argmin())

        # MultiIndex
        kidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        with self.assertRaisesRegex(
            TypeError, "reduction operation 'argmin' not allowed for this dtype"
        ):
            kidx.argmin()

    def test_argmax(self):
        pidx = pd.Index([100, 50, 10, 20, 30, 60, 0, 50, 0, 100, 100, 100, 20, 0, 0])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.argmax(), kidx.argmax())

        # MultiIndex
        kidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        with self.assertRaisesRegex(
            TypeError, "reduction operation 'argmax' not allowed for this dtype"
        ):
            kidx.argmax()

    def test_min(self):
        pidx = pd.Index([3, 2, 1])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.min(), kidx.min())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2)])
        kmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.min(), kmidx.min())

        pidx = pd.DatetimeIndex(["2021-02-01", "2021-01-01", "2021-04-01", "2021-03-01"])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.min(), kidx.min())

    def test_max(self):
        pidx = pd.Index([3, 2, 1])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.max(), kidx.max())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2)])
        kmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.max(), kmidx.max())

        pidx = pd.DatetimeIndex(["2021-02-01", "2021-01-01", "2021-04-01", "2021-03-01"])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.max(), kidx.max())

    def test_monotonic(self):
        # test monotonic_increasing & monotonic_decreasing for MultiIndex.
        # Since the Behavior for null value was changed in pandas >= 1.0.0,
        # several cases are tested differently.
        datas = []

        # increasing / decreasing ordered each index level with string
        datas.append([("w", "a"), ("x", "b"), ("y", "c"), ("z", "d")])
        datas.append([("w", "d"), ("x", "c"), ("y", "b"), ("z", "a")])
        datas.append([("z", "a"), ("y", "b"), ("x", "c"), ("w", "d")])
        datas.append([("z", "d"), ("y", "c"), ("x", "b"), ("w", "a")])
        # mixed order each index level with string
        datas.append([("z", "a"), ("x", "b"), ("y", "c"), ("w", "d")])
        datas.append([("z", "a"), ("y", "c"), ("x", "b"), ("w", "d")])

        # increasing / decreasing ordered each index level with integer
        datas.append([(1, 100), (2, 200), (3, 300), (4, 400), (5, 500)])
        datas.append([(1, 500), (2, 400), (3, 300), (4, 200), (5, 100)])
        datas.append([(5, 100), (4, 200), (3, 300), (2, 400), (1, 500)])
        datas.append([(5, 500), (4, 400), (3, 300), (2, 200), (1, 100)])
        # mixed order each index level with integer
        datas.append([(1, 500), (3, 400), (2, 300), (4, 200), (5, 100)])
        datas.append([(1, 100), (2, 300), (3, 200), (4, 400), (5, 500)])

        # integer / negative mixed tests
        datas.append([("a", -500), ("b", -400), ("c", -300), ("d", -200), ("e", -100)])
        datas.append([("e", -500), ("d", -400), ("c", -300), ("b", -200), ("a", -100)])
        datas.append([(-5, "a"), (-4, "b"), (-3, "c"), (-2, "d"), (-1, "e")])
        datas.append([(-5, "e"), (-4, "d"), (-3, "c"), (-2, "b"), (-1, "a")])
        datas.append([(-5, "e"), (-3, "d"), (-2, "c"), (-4, "b"), (-1, "a")])
        datas.append([(-5, "e"), (-4, "c"), (-3, "b"), (-2, "d"), (-1, "a")])

        # boolean type tests
        datas.append([(True, True), (True, True)])
        datas.append([(True, True), (True, False)])
        datas.append([(True, False), (True, True)])
        datas.append([(False, True), (False, True)])
        datas.append([(False, True), (False, False)])
        datas.append([(False, False), (False, True)])
        datas.append([(True, True), (False, True)])
        datas.append([(True, True), (False, False)])
        datas.append([(True, False), (False, True)])
        datas.append([(False, True), (True, True)])
        datas.append([(False, True), (True, False)])
        datas.append([(False, False), (True, True)])

        # duplicated index value tests
        datas.append([("x", "d"), ("y", "c"), ("y", "b"), ("z", "a")])
        datas.append([("x", "d"), ("y", "b"), ("y", "c"), ("z", "a")])

        # more depth tests
        datas.append([("x", "d", "o"), ("y", "c", "p"), ("y", "c", "q"), ("z", "a", "r")])
        datas.append([("x", "d", "o"), ("y", "c", "q"), ("y", "c", "p"), ("z", "a", "r")])

        # None type tests (None type is treated as False from pandas >= 1.1.4)
        # Refer https://github.com/pandas-dev/pandas/issues/37220
        datas.append([(1, 100), (2, 200), (None, 300), (4, 400), (5, 500)])
        datas.append([(1, 100), (2, 200), (None, None), (4, 400), (5, 500)])
        datas.append([("x", "d"), ("y", "c"), ("y", None), ("z", "a")])
        datas.append([("x", "d"), ("y", "c"), ("y", "b"), (None, "a")])
        datas.append([("x", "d"), ("y", "b"), ("y", "c"), (None, "a")])
        datas.append([("x", "d", "o"), ("y", "c", "p"), ("y", "c", None), ("z", "a", "r")])

        for data in datas:
            with self.subTest(data=data):
                pmidx = pd.MultiIndex.from_tuples(data)
                kmidx = ps.from_pandas(pmidx)
                self.assert_eq(kmidx.is_monotonic_increasing, pmidx.is_monotonic_increasing)
                self.assert_eq(kmidx.is_monotonic_decreasing, pmidx.is_monotonic_decreasing)

        # datas below return different result depends on pandas version.
        # Because the behavior of handling null values is changed in pandas >= 1.1.4.
        # Since Koalas follows latest pandas, all of them should return `False`.
        datas = []
        datas.append([(1, 100), (2, 200), (3, None), (4, 400), (5, 500)])
        datas.append([(1, None), (2, 200), (3, 300), (4, 400), (5, 500)])
        datas.append([(1, 100), (2, 200), (3, 300), (4, 400), (5, None)])
        datas.append([(False, None), (True, True)])
        datas.append([(None, False), (True, True)])
        datas.append([(False, False), (True, None)])
        datas.append([(False, False), (None, True)])
        datas.append([("x", "d"), ("y", None), ("y", None), ("z", "a")])
        datas.append([("x", "d", "o"), ("y", "c", None), ("y", "c", None), ("z", "a", "r")])
        datas.append([(1, 100), (2, 200), (3, 300), (4, 400), (None, 500)])
        datas.append([(1, 100), (2, 200), (3, 300), (4, 400), (None, None)])
        datas.append([(5, 100), (4, 200), (3, None), (2, 400), (1, 500)])
        datas.append([(5, None), (4, 200), (3, 300), (2, 400), (1, 500)])
        datas.append([(5, 100), (4, 200), (3, None), (2, 400), (1, 500)])
        datas.append([(5, 100), (4, 200), (3, 300), (2, 400), (1, None)])
        datas.append([(True, None), (True, True)])
        datas.append([(None, True), (True, True)])
        datas.append([(True, True), (None, True)])
        datas.append([(True, True), (True, None)])
        datas.append([(None, 100), (2, 200), (3, 300), (4, 400), (5, 500)])
        datas.append([(None, None), (2, 200), (3, 300), (4, 400), (5, 500)])
        datas.append([("x", "d"), ("y", None), ("y", "c"), ("z", "a")])
        datas.append([("x", "d", "o"), ("y", "c", None), ("y", "c", "q"), ("z", "a", "r")])

        for data in datas:
            with self.subTest(data=data):
                pmidx = pd.MultiIndex.from_tuples(data)
                kmidx = ps.from_pandas(pmidx)
                if LooseVersion(pd.__version__) < LooseVersion("1.1.4"):
                    self.assert_eq(kmidx.is_monotonic_increasing, False)
                    self.assert_eq(kmidx.is_monotonic_decreasing, False)
                else:
                    self.assert_eq(kmidx.is_monotonic_increasing, pmidx.is_monotonic_increasing)
                    self.assert_eq(kmidx.is_monotonic_decreasing, pmidx.is_monotonic_decreasing)

        # The datas below are tested another way since they cannot be an arguments for
        # `MultiIndex.from_tuples` in pandas >= 1.1.0.
        # Refer https://github.com/databricks/koalas/pull/1688#issuecomment-667156560 for detail.
        if LooseVersion(pd.__version__) < LooseVersion("1.1.0"):
            pmidx = pd.MultiIndex.from_tuples(
                [(-5, None), (-4, None), (-3, None), (-2, None), (-1, None)]
            )
            kmidx = ps.from_pandas(pmidx)
            self.assert_eq(kmidx.is_monotonic_increasing, False)
            self.assert_eq(kmidx.is_monotonic_decreasing, False)

            pmidx = pd.MultiIndex.from_tuples(
                [(None, "e"), (None, "c"), (None, "b"), (None, "d"), (None, "a")]
            )
            kmidx = ps.from_pandas(pmidx)
            self.assert_eq(kmidx.is_monotonic_increasing, False)
            self.assert_eq(kmidx.is_monotonic_decreasing, False)

            pmidx = pd.MultiIndex.from_tuples(
                [(None, None), (None, None), (None, None), (None, None), (None, None)]
            )
            kmidx = ps.from_pandas(pmidx)
            self.assert_eq(kmidx.is_monotonic_increasing, False)
            self.assert_eq(kmidx.is_monotonic_decreasing, False)

            pmidx = pd.MultiIndex.from_tuples([(None, None)])
            kmidx = ps.from_pandas(pmidx)
            self.assert_eq(kmidx.is_monotonic_increasing, False)
            self.assert_eq(kmidx.is_monotonic_decreasing, False)

        # Disable the test cases below because pandas returns `True` or `False` randomly.
        # else:
            # [(-5, None), (-4, None), (-3, None), (-2, None), (-1, None)]
            # kdf = ps.DataFrame({"a": [-5, -4, -3, -2, -1], "b": [1, 1, 1, 1, 1]})
            # kdf["b"] = None
            # kmidx = kdf.set_index(["a", "b"]).index
            # pmidx = kmidx.to_pandas()
            # self.assert_eq(kmidx.is_monotonic_increasing, pmidx.is_monotonic_increasing)
            # self.assert_eq(kmidx.is_monotonic_decreasing, pmidx.is_monotonic_decreasing)

            # [(None, "e"), (None, "c"), (None, "b"), (None, "d"), (None, "a")]
            # kdf = ps.DataFrame({"a": [1, 1, 1, 1, 1], "b": ["e", "c", "b", "d", "a"]})
            # kdf["a"] = None
            # kmidx = kdf.set_index(["a", "b"]).index
            # pmidx = kmidx.to_pandas()
            # self.assert_eq(kmidx.is_monotonic_increasing, pmidx.is_monotonic_increasing)
            # self.assert_eq(kmidx.is_monotonic_decreasing, pmidx.is_monotonic_decreasing)

            # [(None, None), (None, None), (None, None), (None, None), (None, None)]
            # kdf = ps.DataFrame({"a": [1, 1, 1, 1, 1], "b": [1, 1, 1, 1, 1]})
            # kdf["a"] = None
            # kdf["b"] = None
            # kmidx = kdf.set_index(["a", "b"]).index
            # pmidx = kmidx.to_pandas()
            # self.assert_eq(kmidx.is_monotonic_increasing, pmidx.is_monotonic_increasing)
            # self.assert_eq(kmidx.is_monotonic_decreasing, pmidx.is_monotonic_decreasing)
            # [(None, None)]
            # kdf = ps.DataFrame({"a": [1], "b": [1]})
            # kdf["a"] = None
            # kdf["b"] = None
            # kmidx = kdf.set_index(["a", "b"]).index
            # pmidx = kmidx.to_pandas()
            # self.assert_eq(kmidx.is_monotonic_increasing, pmidx.is_monotonic_increasing)
            # self.assert_eq(kmidx.is_monotonic_decreasing, pmidx.is_monotonic_decreasing)

    def test_difference(self):
        # Index
        pidx1 = pd.Index([1, 2, 3, 4], name="koalas")
        pidx2 = pd.Index([3, 4, 5, 6], name="koalas")
        kidx1 = ps.from_pandas(pidx1)
        kidx2 = ps.from_pandas(pidx2)

        self.assert_eq(kidx1.difference(kidx2).sort_values(), pidx1.difference(pidx2).sort_values())
        self.assert_eq(
            kidx1.difference([3, 4, 5, 6]).sort_values(),
            pidx1.difference([3, 4, 5, 6]).sort_values(),
        )
        self.assert_eq(
            kidx1.difference((3, 4, 5, 6)).sort_values(),
            pidx1.difference((3, 4, 5, 6)).sort_values(),
        )
        self.assert_eq(
            kidx1.difference({3, 4, 5, 6}).sort_values(),
            pidx1.difference({3, 4, 5, 6}).sort_values(),
        )
        self.assert_eq(
            kidx1.difference({3: 1, 4: 2, 5: 3, 6: 4}).sort_values(),
            pidx1.difference({3: 1, 4: 2, 5: 3, 6: 4}).sort_values(),
        )

        # Exceptions for Index
        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            kidx1.difference("1234")
        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            kidx1.difference(1234)
        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            kidx1.difference(12.34)
        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            kidx1.difference(None)
        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            kidx1.difference(np.nan)
        with self.assertRaisesRegex(
            ValueError, "The 'sort' keyword only takes the values of None or True; 1 was passed."
        ):
            kidx1.difference(kidx2, sort=1)

        # MultiIndex
        pidx1 = pd.MultiIndex.from_tuples(
            [("a", "x", 1), ("b", "y", 2), ("c", "z", 3)], names=["hello", "koalas", "world"]
        )
        pidx2 = pd.MultiIndex.from_tuples(
            [("a", "x", 1), ("b", "z", 2), ("k", "z", 3)], names=["hello", "koalas", "world"]
        )
        kidx1 = ps.from_pandas(pidx1)
        kidx2 = ps.from_pandas(pidx2)

        self.assert_eq(kidx1.difference(kidx2).sort_values(), pidx1.difference(pidx2).sort_values())
        self.assert_eq(
            kidx1.difference({("a", "x", 1)}).sort_values(),
            pidx1.difference({("a", "x", 1)}).sort_values(),
        )
        self.assert_eq(
            kidx1.difference({("a", "x", 1): [1, 2, 3]}).sort_values(),
            pidx1.difference({("a", "x", 1): [1, 2, 3]}).sort_values(),
        )

        # Exceptions for MultiIndex
        with self.assertRaisesRegex(TypeError, "other must be a MultiIndex or a list of tuples"):
            kidx1.difference(["b", "z", "2"])

    def test_repeat(self):
        pidx = pd.Index(["a", "b", "c"])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(kidx.repeat(3).sort_values(), pidx.repeat(3).sort_values())
        self.assert_eq(kidx.repeat(0).sort_values(), pidx.repeat(0).sort_values())
        self.assert_eq((kidx + "x").repeat(3).sort_values(), (pidx + "x").repeat(3).sort_values())

        self.assertRaises(ValueError, lambda: kidx.repeat(-1))
        self.assertRaises(TypeError, lambda: kidx.repeat("abc"))

        pmidx = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        kmidx = ps.from_pandas(pmidx)

        self.assert_eq(kmidx.repeat(3).sort_values(), pmidx.repeat(3).sort_values())
        self.assert_eq(kmidx.repeat(0).sort_values(), pmidx.repeat(0).sort_values(), almost=True)

        self.assertRaises(ValueError, lambda: kmidx.repeat(-1))
        self.assertRaises(TypeError, lambda: kmidx.repeat("abc"))

    def test_unique(self):
        pidx = pd.Index(["a", "b", "a"])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(kidx.unique().sort_values(), pidx.unique().sort_values())
        self.assert_eq(kidx.unique().sort_values(), pidx.unique().sort_values())

        pmidx = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("x", "a")])
        kmidx = ps.from_pandas(pmidx)

        self.assert_eq(kmidx.unique().sort_values(), pmidx.unique().sort_values())
        self.assert_eq(kmidx.unique().sort_values(), pmidx.unique().sort_values())

    def test_asof(self):
        # Increasing values
        pidx = pd.Index(["2013-12-31", "2014-01-02", "2014-01-03"])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(kidx.asof("2014-01-01"), pidx.asof("2014-01-01"))
        self.assert_eq(kidx.asof("2014-01-02"), pidx.asof("2014-01-02"))
        self.assert_eq(repr(kidx.asof("1999-01-02")), repr(pidx.asof("1999-01-02")))
        self.assert_eq(kidx.asof("2014-01-04"), pidx.asof("2014-01-04"))

        pidx = pd.DatetimeIndex(["2013-12-31", "2014-01-02", "2014-01-03"])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(kidx.asof("2014-01-01"), pidx.asof("2014-01-01"))
        self.assert_eq(kidx.asof("2014-01-02"), pidx.asof("2014-01-02"))
        self.assert_eq(repr(kidx.asof("1999-01-02")), repr(pidx.asof("1999-01-02")))

        # Decreasing values
        pidx = pd.Index(["2014-01-03", "2014-01-02", "2013-12-31"])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(kidx.asof("2014-01-01"), pidx.asof("2014-01-01"))
        self.assert_eq(kidx.asof("2014-01-02"), pidx.asof("2014-01-02"))
        self.assert_eq(kidx.asof("1999-01-02"), pidx.asof("1999-01-02"))
        self.assert_eq(repr(kidx.asof("2015-01-02")), repr(pidx.asof("2015-01-02")))

        pidx = pd.DatetimeIndex(["2014-01-03", "2014-01-02", "2013-12-31"])
        kidx = ps.from_pandas(pidx)

        # TODO: a pandas bug?
        # self.assert_eq(kidx.asof("2014-01-01"), pidx.asof("2014-01-01"))
        # self.assert_eq(kidx.asof("2014-01-02"), pidx.asof("2014-01-02"))
        # self.assert_eq(kidx.asof("1999-01-02"), pidx.asof("1999-01-02"))
        # self.assert_eq(repr(kidx.asof("2015-01-02")), repr(pidx.asof("2015-01-02")))
        self.assert_eq(kidx.asof("2014-01-01"), pd.Timestamp("2014-01-02 00:00:00"))
        self.assert_eq(kidx.asof("2014-01-02"), pd.Timestamp("2014-01-02 00:00:00"))
        self.assert_eq(kidx.asof("1999-01-02"), pd.Timestamp("2013-12-31 00:00:00"))
        self.assert_eq(repr(kidx.asof("2015-01-02")), repr(pd.NaT))

        # Not increasing, neither decreasing (ValueError)
        kidx = ps.Index(["2013-12-31", "2015-01-02", "2014-01-03"])
        self.assertRaises(ValueError, lambda: kidx.asof("2013-12-31"))

        kmidx = ps.MultiIndex.from_tuples([("a", "a"), ("a", "b"), ("a", "c")])
        self.assertRaises(NotImplementedError, lambda: kmidx.asof(("a", "b")))

    def test_union(self):
        # Index
        pidx1 = pd.Index([1, 2, 3, 4])
        pidx2 = pd.Index([3, 4, 5, 6])
        kidx1 = ps.from_pandas(pidx1)
        kidx2 = ps.from_pandas(pidx2)

        self.assert_eq(kidx1.union(kidx2), pidx1.union(pidx2))
        self.assert_eq(kidx2.union(kidx1), pidx2.union(pidx1))
        self.assert_eq(kidx1.union([3, 4, 5, 6]), pidx1.union([3, 4, 5, 6]), almost=True)
        self.assert_eq(kidx2.union([1, 2, 3, 4]), pidx2.union([1, 2, 3, 4]), almost=True)
        self.assert_eq(
            kidx1.union(ps.Series([3, 4, 5, 6])), pidx1.union(pd.Series([3, 4, 5, 6])), almost=True
        )
        self.assert_eq(
            kidx2.union(ps.Series([1, 2, 3, 4])), pidx2.union(pd.Series([1, 2, 3, 4])), almost=True
        )

        # Testing if the result is correct after sort=False.
        # The `sort` argument is added in pandas 0.24.
        if LooseVersion(pd.__version__) >= LooseVersion("0.24"):
            self.assert_eq(
                kidx1.union(kidx2, sort=False).sort_values(),
                pidx1.union(pidx2, sort=False).sort_values(),
            )
            self.assert_eq(
                kidx2.union(kidx1, sort=False).sort_values(),
                pidx2.union(pidx1, sort=False).sort_values(),
            )
            self.assert_eq(
                kidx1.union([3, 4, 5, 6], sort=False).sort_values(),
                pidx1.union([3, 4, 5, 6], sort=False).sort_values(),
                almost=True,
            )
            self.assert_eq(
                kidx2.union([1, 2, 3, 4], sort=False).sort_values(),
                pidx2.union([1, 2, 3, 4], sort=False).sort_values(),
                almost=True,
            )
            self.assert_eq(
                kidx1.union(ps.Series([3, 4, 5, 6]), sort=False).sort_values(),
                pidx1.union(pd.Series([3, 4, 5, 6]), sort=False).sort_values(),
                almost=True,
            )
            self.assert_eq(
                kidx2.union(ps.Series([1, 2, 3, 4]), sort=False).sort_values(),
                pidx2.union(pd.Series([1, 2, 3, 4]), sort=False).sort_values(),
                almost=True,
            )

        # Duplicated values for Index is supported in pandas >= 1.0.0
        if LooseVersion(pd.__version__) >= LooseVersion("1.0.0"):
            pidx1 = pd.Index([1, 2, 3, 4, 3, 4, 3, 4])
            pidx2 = pd.Index([3, 4, 3, 4, 5, 6])
            kidx1 = ps.from_pandas(pidx1)
            kidx2 = ps.from_pandas(pidx2)

            self.assert_eq(kidx1.union(kidx2), pidx1.union(pidx2))
            self.assert_eq(kidx2.union(kidx1), pidx2.union(pidx1))
            self.assert_eq(
                kidx1.union([3, 4, 3, 3, 5, 6]), pidx1.union([3, 4, 3, 4, 5, 6]), almost=True
            )
            self.assert_eq(
                kidx2.union([1, 2, 3, 4, 3, 4, 3, 4]),
                pidx2.union([1, 2, 3, 4, 3, 4, 3, 4]),
                almost=True,
            )
            self.assert_eq(
                kidx1.union(ps.Series([3, 4, 3, 3, 5, 6])),
                pidx1.union(pd.Series([3, 4, 3, 4, 5, 6])),
                almost=True,
            )
            self.assert_eq(
                kidx2.union(ps.Series([1, 2, 3, 4, 3, 4, 3, 4])),
                pidx2.union(pd.Series([1, 2, 3, 4, 3, 4, 3, 4])),
                almost=True,
            )

        # MultiIndex
        pmidx1 = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("x", "a"), ("x", "b")])
        pmidx2 = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("x", "c"), ("x", "d")])
        pmidx3 = pd.MultiIndex.from_tuples([(1, 1), (1, 2), (1, 3), (1, 4), (1, 3), (1, 4)])
        pmidx4 = pd.MultiIndex.from_tuples([(1, 3), (1, 4), (1, 5), (1, 6)])
        kmidx1 = ps.from_pandas(pmidx1)
        kmidx2 = ps.from_pandas(pmidx2)
        kmidx3 = ps.from_pandas(pmidx3)
        kmidx4 = ps.from_pandas(pmidx4)

        self.assert_eq(kmidx1.union(kmidx2), pmidx1.union(pmidx2))
        self.assert_eq(kmidx2.union(kmidx1), pmidx2.union(pmidx1))
        self.assert_eq(kmidx3.union(kmidx4), pmidx3.union(pmidx4))
        self.assert_eq(kmidx4.union(kmidx3), pmidx4.union(pmidx3))
        self.assert_eq(
            kmidx1.union([("x", "a"), ("x", "b"), ("x", "c"), ("x", "d")]),
            pmidx1.union([("x", "a"), ("x", "b"), ("x", "c"), ("x", "d")]),
        )
        self.assert_eq(
            kmidx2.union([("x", "a"), ("x", "b"), ("x", "a"), ("x", "b")]),
            pmidx2.union([("x", "a"), ("x", "b"), ("x", "a"), ("x", "b")]),
        )
        self.assert_eq(
            kmidx3.union([(1, 3), (1, 4), (1, 5), (1, 6)]),
            pmidx3.union([(1, 3), (1, 4), (1, 5), (1, 6)]),
        )
        self.assert_eq(
            kmidx4.union([(1, 1), (1, 2), (1, 3), (1, 4), (1, 3), (1, 4)]),
            pmidx4.union([(1, 1), (1, 2), (1, 3), (1, 4), (1, 3), (1, 4)]),
        )

        # Testing if the result is correct after sort=False.
        # The `sort` argument is added in pandas 0.24.
        if LooseVersion(pd.__version__) >= LooseVersion("0.24"):
            self.assert_eq(
                kmidx1.union(kmidx2, sort=False).sort_values(),
                pmidx1.union(pmidx2, sort=False).sort_values(),
            )
            self.assert_eq(
                kmidx2.union(kmidx1, sort=False).sort_values(),
                pmidx2.union(pmidx1, sort=False).sort_values(),
            )
            self.assert_eq(
                kmidx3.union(kmidx4, sort=False).sort_values(),
                pmidx3.union(pmidx4, sort=False).sort_values(),
            )
            self.assert_eq(
                kmidx4.union(kmidx3, sort=False).sort_values(),
                pmidx4.union(pmidx3, sort=False).sort_values(),
            )
            self.assert_eq(
                kmidx1.union(
                    [("x", "a"), ("x", "b"), ("x", "c"), ("x", "d")], sort=False
                ).sort_values(),
                pmidx1.union(
                    [("x", "a"), ("x", "b"), ("x", "c"), ("x", "d")], sort=False
                ).sort_values(),
            )
            self.assert_eq(
                kmidx2.union(
                    [("x", "a"), ("x", "b"), ("x", "a"), ("x", "b")], sort=False
                ).sort_values(),
                pmidx2.union(
                    [("x", "a"), ("x", "b"), ("x", "a"), ("x", "b")], sort=False
                ).sort_values(),
            )
            self.assert_eq(
                kmidx3.union([(1, 3), (1, 4), (1, 5), (1, 6)], sort=False).sort_values(),
                pmidx3.union([(1, 3), (1, 4), (1, 5), (1, 6)], sort=False).sort_values(),
            )
            self.assert_eq(
                kmidx4.union(
                    [(1, 1), (1, 2), (1, 3), (1, 4), (1, 3), (1, 4)], sort=False
                ).sort_values(),
                pmidx4.union(
                    [(1, 1), (1, 2), (1, 3), (1, 4), (1, 3), (1, 4)], sort=False
                ).sort_values(),
            )

        self.assertRaises(NotImplementedError, lambda: kidx1.union(kmidx1))
        self.assertRaises(TypeError, lambda: kmidx1.union(kidx1))
        self.assertRaises(TypeError, lambda: kmidx1.union(["x", "a"]))
        self.assertRaises(ValueError, lambda: kidx1.union(ps.range(2)))

    def test_take(self):
        # Index
        pidx = pd.Index([100, 200, 300, 400, 500], name="Koalas")
        kidx = ps.from_pandas(pidx)

        self.assert_eq(kidx.take([0, 2, 4]).sort_values(), pidx.take([0, 2, 4]).sort_values())
        self.assert_eq(
            kidx.take(range(0, 5, 2)).sort_values(), pidx.take(range(0, 5, 2)).sort_values()
        )
        self.assert_eq(kidx.take([-4, -2, 0]).sort_values(), pidx.take([-4, -2, 0]).sort_values())
        self.assert_eq(
            kidx.take(range(-4, 1, 2)).sort_values(), pidx.take(range(-4, 1, 2)).sort_values()
        )

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples(
            [("x", "a"), ("x", "b"), ("x", "c")], names=["hello", "Koalas"]
        )
        kmidx = ps.from_pandas(pmidx)

        self.assert_eq(kmidx.take([0, 2]).sort_values(), pmidx.take([0, 2]).sort_values())
        self.assert_eq(
            kmidx.take(range(0, 4, 2)).sort_values(), pmidx.take(range(0, 4, 2)).sort_values()
        )
        self.assert_eq(kmidx.take([-2, 0]).sort_values(), pmidx.take([-2, 0]).sort_values())
        self.assert_eq(
            kmidx.take(range(-2, 1, 2)).sort_values(), pmidx.take(range(-2, 1, 2)).sort_values()
        )

        # Checking the type of indices.
        self.assertRaises(TypeError, lambda: kidx.take(1))
        self.assertRaises(TypeError, lambda: kidx.take("1"))
        self.assertRaises(TypeError, lambda: kidx.take({1, 2}))
        self.assertRaises(TypeError, lambda: kidx.take({1: None, 2: None}))
        self.assertRaises(TypeError, lambda: kmidx.take(1))
        self.assertRaises(TypeError, lambda: kmidx.take("1"))
        self.assertRaises(TypeError, lambda: kmidx.take({1, 2}))
        self.assertRaises(TypeError, lambda: kmidx.take({1: None, 2: None}))

    def test_index_get_level_values(self):
        pidx = pd.Index([1, 2, 3], name="ks")
        kidx = ps.from_pandas(pidx)

        for level in [0, "ks"]:
            self.assert_eq(kidx.get_level_values(level), pidx.get_level_values(level))

    def test_multiindex_get_level_values(self):
        pmidx = pd.MultiIndex.from_tuples([("a", "d"), ("b", "e"), ("c", "f")])
        pmidx.names = ["level_1", "level_2"]
        kmidx = ps.from_pandas(pmidx)

        for level in [0, 1, "level_1", "level_2"]:
            self.assert_eq(kmidx.get_level_values(level), pmidx.get_level_values(level))

    def test_index_get_level_number(self):
        # name of two levels are the same, which is None
        kdf = ps.DataFrame({"a": [1, 2, 3]}, index=[list("aac"), list("ddf")])
        with self.assertRaisesRegex(
            ValueError, "The name None occurs multiple times, use a level number"
        ):
            kdf.index._get_level_number(None)

        mi = pd.MultiIndex.from_arrays((list("abc"), list("def")))
        mi.names = ["level_1", "level_2"]
        kdf = ps.DataFrame({"a": [1, 2, 3]}, index=mi)

        # level is not int and not in the level name list
        with self.assertRaisesRegex(KeyError, "Level lv_3 not found"):
            kdf.index._get_level_number("lv_3")

        # level is int, but an invalid negative number
        with self.assertRaisesRegex(IndexError, "Too many levels: Index has only"):
            kdf.index._get_level_number(-3)

        # level is int, but an invalid positive number
        with self.assertRaisesRegex(IndexError, "Too many levels: Index has only"):
            kdf.index._get_level_number(3)

        # Correct and valid inputs in numbers
        level_number = [-2, -1, 0, 1]
        outputs = [0, 1, 0, 1]

        for lv, output in zip(level_number, outputs):
            self.assertEqual(output, kdf.index._get_level_number(lv))

        # Valid inputs as level names
        level_names = ["level_1", "level_2"]
        outputs = [0, 1]

        for lv, output in zip(level_names, outputs):
            self.assertEqual(output, kdf.index._get_level_number(lv))

    def test_holds_integer(self):
        pidx = pd.Index([1, 2, 3, 4])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.holds_integer(), kidx.holds_integer())

        pidx = pd.Index([1.1, 2.2, 3.3, 4.4])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.holds_integer(), kidx.holds_integer())

        pidx = pd.Index(["A", "B", "C", "D"])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.holds_integer(), kidx.holds_integer())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "a")])
        kmidx = ps.from_pandas(pmidx)
        self.assert_eq(pmidx.holds_integer(), kmidx.holds_integer())

        pmidx = pd.MultiIndex.from_tuples([(10, 1), (10, 2), (20, 1)])
        kmidx = ps.from_pandas(pmidx)
        self.assert_eq(pmidx.holds_integer(), kmidx.holds_integer())

    def test_abs(self):
        pidx = pd.Index([-2, -1, 0, 1])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(abs(pidx), abs(kidx))
        self.assert_eq(np.abs(pidx), np.abs(kidx))

        kidx = ps.MultiIndex.from_tuples([(1, 2)], names=["level1", "level2"])
        with self.assertRaisesRegex(TypeError, "perform __abs__ with this index"):
            abs(kidx)

    def test_hasnans(self):
        # BooleanType
        pidx = pd.Index([True, False, True, True])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.hasnans, kidx.hasnans)

        pidx = pd.Index([True, False, np.nan, True])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.hasnans, kidx.hasnans)

        # TimestampType
        pser = pd.Series([pd.Timestamp("2020-07-30") for _ in range(3)])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.hasnans, kser.hasnans)

        pser = pd.Series([pd.Timestamp("2020-07-30"), np.nan, pd.Timestamp("2020-07-30")])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.hasnans, kser.hasnans)

        # Not supported for MultiIndex
        kmidx = ps.Index([("a", 1), ("b", 2)])
        self.assertRaises(NotImplementedError, lambda: kmidx.hasnans())

    def test_intersection(self):
        pidx = pd.Index([1, 2, 3, 4], name="Koalas")
        kidx = ps.from_pandas(pidx)

        # other = Index
        pidx_other = pd.Index([3, 4, 5, 6], name="Koalas")
        kidx_other = ps.from_pandas(pidx_other)
        self.assert_eq(pidx.intersection(pidx_other), kidx.intersection(kidx_other).sort_values())
        self.assert_eq(
            (pidx + 1).intersection(pidx_other), (kidx + 1).intersection(kidx_other).sort_values()
        )

        pidx_other_different_name = pd.Index([3, 4, 5, 6], name="Databricks")
        kidx_other_different_name = ps.from_pandas(pidx_other_different_name)
        self.assert_eq(
            pidx.intersection(pidx_other_different_name),
            kidx.intersection(kidx_other_different_name).sort_values(),
        )
        self.assert_eq(
            (pidx + 1).intersection(pidx_other_different_name),
            (kidx + 1).intersection(kidx_other_different_name).sort_values(),
        )

        pidx_other_from_frame = pd.DataFrame({"a": [3, 4, 5, 6]}).set_index("a").index
        kidx_other_from_frame = ps.from_pandas(pidx_other_from_frame)
        self.assert_eq(
            pidx.intersection(pidx_other_from_frame),
            kidx.intersection(kidx_other_from_frame).sort_values(),
        )
        self.assert_eq(
            (pidx + 1).intersection(pidx_other_from_frame),
            (kidx + 1).intersection(kidx_other_from_frame).sort_values(),
        )

        # other = MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        kmidx = ps.from_pandas(pmidx)
        if LooseVersion(pd.__version__) < LooseVersion("1.2.0"):
            self.assert_eq(
                kidx.intersection(kmidx).sort_values(),
                kidx._kdf.head(0).index.rename(None),
                almost=True,
            )
            self.assert_eq(
                (kidx + 1).intersection(kmidx).sort_values(),
                kidx._kdf.head(0).index.rename(None),
                almost=True,
            )
        else:
            self.assert_eq(
                pidx.intersection(pmidx), kidx.intersection(kmidx).sort_values(), almost=True
            )
            self.assert_eq(
                (pidx + 1).intersection(pmidx),
                (kidx + 1).intersection(kmidx).sort_values(),
                almost=True,
            )

        # other = Series
        pser = pd.Series([3, 4, 5, 6])
        kser = ps.from_pandas(pser)
        if LooseVersion(pd.__version__) < LooseVersion("1.2.0"):
            self.assert_eq(kidx.intersection(kser).sort_values(), ps.Index([3, 4], name="Koalas"))
            self.assert_eq(
                (kidx + 1).intersection(kser).sort_values(), ps.Index([3, 4, 5], name="Koalas")
            )
        else:
            self.assert_eq(pidx.intersection(pser), kidx.intersection(kser).sort_values())
            self.assert_eq(
                (pidx + 1).intersection(pser), (kidx + 1).intersection(kser).sort_values()
            )

        pser_different_name = pd.Series([3, 4, 5, 6], name="Databricks")
        kser_different_name = ps.from_pandas(pser_different_name)
        if LooseVersion(pd.__version__) < LooseVersion("1.2.0"):
            self.assert_eq(
                kidx.intersection(kser_different_name).sort_values(),
                ps.Index([3, 4], name="Koalas"),
            )
            self.assert_eq(
                (kidx + 1).intersection(kser_different_name).sort_values(),
                ps.Index([3, 4, 5], name="Koalas"),
            )
        else:
            self.assert_eq(
                pidx.intersection(pser_different_name),
                kidx.intersection(kser_different_name).sort_values(),
            )
            self.assert_eq(
                (pidx + 1).intersection(pser_different_name),
                (kidx + 1).intersection(kser_different_name).sort_values(),
            )

        others = ([3, 4, 5, 6], (3, 4, 5, 6), {3: None, 4: None, 5: None, 6: None})
        for other in others:
            if LooseVersion(pd.__version__) < LooseVersion("1.2.0"):
                self.assert_eq(
                    kidx.intersection(other).sort_values(), ps.Index([3, 4], name="Koalas")
                )
                self.assert_eq(
                    (kidx + 1).intersection(other).sort_values(), ps.Index([3, 4, 5], name="Koalas")
                )
            else:
                self.assert_eq(pidx.intersection(other), kidx.intersection(other).sort_values())
                self.assert_eq(
                    (pidx + 1).intersection(other), (kidx + 1).intersection(other).sort_values()
                )

        # MultiIndex / other = Index
        self.assert_eq(
            pmidx.intersection(pidx), kmidx.intersection(kidx).sort_values(), almost=True
        )
        self.assert_eq(
            pmidx.intersection(pidx_other_from_frame),
            kmidx.intersection(kidx_other_from_frame).sort_values(),
            almost=True,
        )

        # MultiIndex / other = MultiIndex
        pmidx_other = pd.MultiIndex.from_tuples([("c", "z"), ("d", "w")])
        kmidx_other = ps.from_pandas(pmidx_other)
        self.assert_eq(
            pmidx.intersection(pmidx_other), kmidx.intersection(kmidx_other).sort_values()
        )

        # MultiIndex / other = list
        other = [("c", "z"), ("d", "w")]
        self.assert_eq(pmidx.intersection(other), kmidx.intersection(other).sort_values())

        # MultiIndex / other = tuple
        other = (("c", "z"), ("d", "w"))
        self.assert_eq(pmidx.intersection(other), kmidx.intersection(other).sort_values())

        # MultiIndex / other = dict
        other = {("c", "z"): None, ("d", "w"): None}
        self.assert_eq(pmidx.intersection(other), kmidx.intersection(other).sort_values())

        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            kidx.intersection(4)
        with self.assertRaisesRegex(TypeError, "other must be a MultiIndex or a list of tuples"):
            kmidx.intersection(4)
        with self.assertRaisesRegex(TypeError, "other must be a MultiIndex or a list of tuples"):
            kmidx.intersection(ps.Series([3, 4, 5, 6]))
        with self.assertRaisesRegex(ValueError, "Index data must be 1-dimensional"):
            kidx.intersection(ps.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}))
        with self.assertRaisesRegex(ValueError, "Index data must be 1-dimensional"):
            kmidx.intersection(ps.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}))

    def test_item(self):
        pidx = pd.Index([10])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.item(), kidx.item())

        # with timestamp
        pidx = pd.Index([datetime(1990, 3, 9)])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.item(), kidx.item())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x")])
        kmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.item(), kmidx.item())

        # MultiIndex with timestamp
        pmidx = pd.MultiIndex.from_tuples([(datetime(1990, 3, 9), datetime(2019, 8, 15))])
        kmidx = ps.from_pandas(pmidx)

        self.assert_eq(pidx.item(), kidx.item())

        err_msg = "can only convert an array of size 1 to a Python scalar"
        with self.assertRaisesRegex(ValueError, err_msg):
            ps.Index([10, 20]).item()
        with self.assertRaisesRegex(ValueError, err_msg):
            ps.MultiIndex.from_tuples([("a", "x"), ("b", "y")]).item()

    def test_inferred_type(self):
        # Integer
        pidx = pd.Index([1, 2, 3])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.inferred_type, kidx.inferred_type)

        # Floating
        pidx = pd.Index([1.0, 2.0, 3.0])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.inferred_type, kidx.inferred_type)

        # String
        pidx = pd.Index(["a", "b", "c"])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.inferred_type, kidx.inferred_type)

        # Boolean
        pidx = pd.Index([True, False, True, False])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.inferred_type, kidx.inferred_type)

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x")])
        kmidx = ps.from_pandas(pmidx)
        self.assert_eq(pmidx.inferred_type, kmidx.inferred_type)

    def test_multi_index_from_index(self):
        tuples = [(1, "red"), (1, "blue"), (2, "red"), (2, "blue")]
        pmidx = pd.Index(tuples)
        kmidx = ps.Index(tuples)

        self.assertTrue(isinstance(kmidx, ps.MultiIndex))
        self.assert_eq(pmidx, kmidx)

        # Specify the `names`
        pmidx = pd.Index(tuples, names=["Hello", "Koalas"])
        kmidx = ps.Index(tuples, names=["Hello", "Koalas"])

        self.assertTrue(isinstance(kmidx, ps.MultiIndex))
        self.assert_eq(pmidx, kmidx)

    @unittest.skipIf(
        LooseVersion(pd.__version__) < LooseVersion("0.24"),
        "MultiIndex.from_frame is new in pandas 0.24",
    )
    def test_multiindex_from_frame(self):
        pdf = pd.DataFrame(
            [["HI", "Temp"], ["HI", "Precip"], ["NJ", "Temp"], ["NJ", "Precip"]], columns=["a", "b"]
        )
        kdf = ps.from_pandas(pdf)
        pidx = pd.MultiIndex.from_frame(pdf)
        kidx = ps.MultiIndex.from_frame(kdf)

        self.assert_eq(pidx, kidx)

        # Specify `names`
        pidx = pd.MultiIndex.from_frame(pdf, names=["state", "observation"])
        kidx = ps.MultiIndex.from_frame(kdf, names=["state", "observation"])
        self.assert_eq(pidx, kidx)

        pidx = pd.MultiIndex.from_frame(pdf, names=("state", "observation"))
        kidx = ps.MultiIndex.from_frame(kdf, names=("state", "observation"))
        self.assert_eq(pidx, kidx)

        # MultiIndex columns
        pidx = pd.MultiIndex.from_tuples([("a", "w"), ("b", "x")])
        pdf.columns = pidx
        kdf = ps.from_pandas(pdf)

        pidx = pd.MultiIndex.from_frame(pdf)
        kidx = ps.MultiIndex.from_frame(kdf)

        self.assert_eq(pidx, kidx)

        # tuples for names
        pidx = pd.MultiIndex.from_frame(pdf, names=[("a", "w"), ("b", "x")])
        kidx = ps.MultiIndex.from_frame(kdf, names=[("a", "w"), ("b", "x")])

        self.assert_eq(pidx, kidx)

        err_msg = "Input must be a DataFrame"
        with self.assertRaisesRegex(TypeError, err_msg):
            ps.MultiIndex.from_frame({"a": [1, 2, 3], "b": [4, 5, 6]})

        self.assertRaises(TypeError, lambda: ps.MultiIndex.from_frame(kdf, names="ab"))

        # non-string names
        self.assert_eq(
            ps.MultiIndex.from_frame(kdf, names=[0, 1]), pd.MultiIndex.from_frame(pdf, names=[0, 1])
        )
        self.assert_eq(
            ps.MultiIndex.from_frame(kdf, names=[("x", 0), ("y", 1)]),
            pd.MultiIndex.from_frame(pdf, names=[("x", 0), ("y", 1)]),
        )

        pdf = pd.DataFrame([["HI", "Temp"], ["HI", "Precip"], ["NJ", "Temp"], ["NJ", "Precip"]])
        kdf = ps.from_pandas(pdf)
        self.assert_eq(ps.MultiIndex.from_frame(kdf), pd.MultiIndex.from_frame(pdf))

    def test_is_type_compatible(self):
        data_types = ["integer", "floating", "string", "boolean"]
        # Integer
        pidx = pd.Index([1, 2, 3])
        kidx = ps.from_pandas(pidx)
        for data_type in data_types:
            self.assert_eq(pidx.is_type_compatible(data_type), kidx.is_type_compatible(data_type))

        # Floating
        pidx = pd.Index([1.0, 2.0, 3.0])
        kidx = ps.from_pandas(pidx)
        for data_type in data_types:
            self.assert_eq(pidx.is_type_compatible(data_type), kidx.is_type_compatible(data_type))

        # String
        pidx = pd.Index(["a", "b", "c"])
        kidx = ps.from_pandas(pidx)
        for data_type in data_types:
            self.assert_eq(pidx.is_type_compatible(data_type), kidx.is_type_compatible(data_type))

        # Boolean
        pidx = pd.Index([True, False, True, False])
        kidx = ps.from_pandas(pidx)
        for data_type in data_types:
            self.assert_eq(pidx.is_type_compatible(data_type), kidx.is_type_compatible(data_type))

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x")])
        kmidx = ps.from_pandas(pmidx)
        for data_type in data_types:
            self.assert_eq(pmidx.is_type_compatible(data_type), kmidx.is_type_compatible(data_type))

    def test_asi8(self):
        # Integer
        pidx = pd.Index([1, 2, 3])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.asi8, kidx.asi8)
        self.assert_eq(pidx.astype("int").asi8, kidx.astype("int").asi8)
        self.assert_eq(pidx.astype("int16").asi8, kidx.astype("int16").asi8)
        self.assert_eq(pidx.astype("int8").asi8, kidx.astype("int8").asi8)

        # Integer with missing value
        pidx = pd.Index([1, 2, None, 4, 5])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.asi8, kidx.asi8)

        # Datetime
        pidx = pd.date_range(end="1/1/2018", periods=3)
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.asi8, kidx.asi8)

        # Floating
        pidx = pd.Index([1.0, 2.0, 3.0])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.asi8, kidx.asi8)

        # String
        pidx = pd.Index(["a", "b", "c"])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.asi8, kidx.asi8)

        # Boolean
        pidx = pd.Index([True, False, True, False])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.asi8, kidx.asi8)

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([(1, 2)])
        kmidx = ps.from_pandas(pmidx)
        self.assert_eq(pmidx.asi8, kmidx.asi8)

    def test_index_is_unique(self):
        indexes = [("a", "b", "c"), ("a", "a", "c"), (1, 3, 3), (1, 2, 3)]
        names = [None, "ks", "ks", None]
        is_uniq = [True, False, False, True]

        for idx, name, expected in zip(indexes, names, is_uniq):
            pdf = pd.DataFrame({"a": [1, 2, 3]}, index=pd.Index(idx, name=name))
            kdf = ps.from_pandas(pdf)

            self.assertEqual(kdf.index.is_unique, expected)

    def test_multiindex_is_unique(self):
        indexes = [
            [list("abc"), list("edf")],
            [list("aac"), list("edf")],
            [list("aac"), list("eef")],
            [[1, 4, 4], [4, 6, 6]],
        ]
        is_uniq = [True, True, False, False]

        for idx, expected in zip(indexes, is_uniq):
            pdf = pd.DataFrame({"a": [1, 2, 3]}, index=idx)
            kdf = ps.from_pandas(pdf)

            self.assertEqual(kdf.index.is_unique, expected)

    def test_view(self):
        pidx = pd.Index([1, 2, 3, 4], name="Koalas")
        kidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.view(), kidx.view())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        kmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.view(), kmidx.view())

    def test_insert(self):
        # Integer
        pidx = pd.Index([1, 2, 3], name="Koalas")
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.insert(1, 100), kidx.insert(1, 100))
        self.assert_eq(pidx.insert(-1, 100), kidx.insert(-1, 100))
        self.assert_eq(pidx.insert(100, 100), kidx.insert(100, 100))
        self.assert_eq(pidx.insert(-100, 100), kidx.insert(-100, 100))

        # Floating
        pidx = pd.Index([1.0, 2.0, 3.0], name="Koalas")
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.insert(1, 100.0), kidx.insert(1, 100.0))
        self.assert_eq(pidx.insert(-1, 100.0), kidx.insert(-1, 100.0))
        self.assert_eq(pidx.insert(100, 100.0), kidx.insert(100, 100.0))
        self.assert_eq(pidx.insert(-100, 100.0), kidx.insert(-100, 100.0))

        # String
        pidx = pd.Index(["a", "b", "c"], name="Koalas")
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.insert(1, "x"), kidx.insert(1, "x"))
        self.assert_eq(pidx.insert(-1, "x"), kidx.insert(-1, "x"))
        self.assert_eq(pidx.insert(100, "x"), kidx.insert(100, "x"))
        self.assert_eq(pidx.insert(-100, "x"), kidx.insert(-100, "x"))

        # Boolean
        pidx = pd.Index([True, False, True, False], name="Koalas")
        kidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.insert(1, True), kidx.insert(1, True))
        self.assert_eq(pidx.insert(-1, True), kidx.insert(-1, True))
        self.assert_eq(pidx.insert(100, True), kidx.insert(100, True))
        self.assert_eq(pidx.insert(-100, True), kidx.insert(-100, True))

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples(
            [("a", "x"), ("b", "y"), ("c", "z")], names=["Hello", "Koalas"]
        )
        kmidx = ps.from_pandas(pmidx)
        self.assert_eq(pmidx.insert(2, ("h", "j")), kmidx.insert(2, ("h", "j")))
        self.assert_eq(pmidx.insert(-1, ("h", "j")), kmidx.insert(-1, ("h", "j")))

        err_msg = "index 4 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            kmidx.insert(4, ("b", "y"))

    def test_astype(self):
        pidx = pd.Index([10, 20, 15, 30, 45], name="x")
        kidx = ps.Index(pidx)

        self.assert_eq(kidx.astype(int), pidx.astype(int))
        self.assert_eq(kidx.astype(np.int), pidx.astype(np.int))
        self.assert_eq(kidx.astype(np.int8), pidx.astype(np.int8))
        self.assert_eq(kidx.astype(np.int16), pidx.astype(np.int16))
        self.assert_eq(kidx.astype(np.int32), pidx.astype(np.int32))
        self.assert_eq(kidx.astype(np.int64), pidx.astype(np.int64))
        self.assert_eq(kidx.astype(np.byte), pidx.astype(np.byte))
        self.assert_eq(kidx.astype("int"), pidx.astype("int"))
        self.assert_eq(kidx.astype("int8"), pidx.astype("int8"))
        self.assert_eq(kidx.astype("int16"), pidx.astype("int16"))
        self.assert_eq(kidx.astype("int32"), pidx.astype("int32"))
        self.assert_eq(kidx.astype("int64"), pidx.astype("int64"))
        self.assert_eq(kidx.astype("b"), pidx.astype("b"))
        self.assert_eq(kidx.astype("byte"), pidx.astype("byte"))
        self.assert_eq(kidx.astype("i"), pidx.astype("i"))
        self.assert_eq(kidx.astype("long"), pidx.astype("long"))
        self.assert_eq(kidx.astype("short"), pidx.astype("short"))
        self.assert_eq(kidx.astype(np.float), pidx.astype(np.float))
        self.assert_eq(kidx.astype(np.float32), pidx.astype(np.float32))
        self.assert_eq(kidx.astype(np.float64), pidx.astype(np.float64))
        self.assert_eq(kidx.astype("float"), pidx.astype("float"))
        self.assert_eq(kidx.astype("float32"), pidx.astype("float32"))
        self.assert_eq(kidx.astype("float64"), pidx.astype("float64"))
        self.assert_eq(kidx.astype("double"), pidx.astype("double"))
        self.assert_eq(kidx.astype("f"), pidx.astype("f"))
        self.assert_eq(kidx.astype(bool), pidx.astype(bool))
        self.assert_eq(kidx.astype("bool"), pidx.astype("bool"))
        self.assert_eq(kidx.astype("?"), pidx.astype("?"))
        self.assert_eq(kidx.astype(np.unicode_), pidx.astype(np.unicode_))
        self.assert_eq(kidx.astype("str"), pidx.astype("str"))
        self.assert_eq(kidx.astype("U"), pidx.astype("U"))

        pidx = pd.Index([10, 20, 15, 30, 45, None], name="x")
        kidx = ps.Index(pidx)

        pidx = pd.Index(["hi", "hi ", " ", " \t", "", None], name="x")
        kidx = ps.Index(pidx)

        self.assert_eq(kidx.astype(bool), pidx.astype(bool))
        self.assert_eq(kidx.astype(str).to_numpy(), ["hi", "hi ", " ", " \t", "", "None"])

        pidx = pd.Index([True, False, None], name="x")
        kidx = ps.Index(pidx)

        self.assert_eq(kidx.astype(bool), pidx.astype(bool))

        pidx = pd.Index(["2020-10-27"], name="x")
        kidx = ps.Index(pidx)

        self.assert_eq(kidx.astype("datetime64[ns]"), pidx.astype("datetime64[ns]"))

        with self.assertRaisesRegex(TypeError, "not understood"):
            kidx.astype("int63")

    def test_to_list(self):
        # Index
        pidx = pd.Index([1, 2, 3, 4, 5])
        kidx = ps.from_pandas(pidx)
        # MultiIndex
        tuples = [(1, "red"), (1, "blue"), (2, "red"), (2, "green")]
        pmidx = pd.MultiIndex.from_tuples(tuples)
        kmidx = ps.from_pandas(pmidx)

        self.assert_eq(kidx.tolist(), pidx.tolist())
        self.assert_eq(kmidx.tolist(), pmidx.tolist())

    def test_index_ops(self):
        pidx = pd.Index([1, 2, 3, 4, 5])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(kidx * 100 + kidx * 10 + kidx, pidx * 100 + pidx * 10 + pidx)

        pidx = pd.Index([1, 2, 3, 4, 5], name="a")
        kidx = ps.from_pandas(pidx)

        self.assert_eq(kidx * 100 + kidx * 10 + kidx, pidx * 100 + pidx * 10 + pidx)

        pdf = pd.DataFrame(
            index=pd.MultiIndex.from_tuples([(1, 2), (3, 4), (5, 6)], names=["a", "b"])
        )
        kdf = ps.from_pandas(pdf)

        pidx1 = pdf.index.get_level_values(0)
        pidx2 = pdf.index.get_level_values(1)
        kidx1 = kdf.index.get_level_values(0)
        kidx2 = kdf.index.get_level_values(1)

        if LooseVersion(pd.__version__) >= LooseVersion("1.0"):
            self.assert_eq(kidx1 * 10 + kidx2, pidx1 * 10 + pidx2)
        else:
            self.assert_eq(kidx1 * 10 + kidx2, (pidx1 * 10 + pidx2).rename(None))

    def test_factorize(self):
        pidx = pd.Index(["a", "b", "a", "b"])
        kidx = ps.from_pandas(pidx)
        pcodes, puniques = pidx.factorize(sort=True)
        kcodes, kuniques = kidx.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pmidx = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        kmidx = ps.from_pandas(pmidx)

        self.assertRaises(PandasNotImplementedError, lambda: kmidx.factorize())


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_base import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
